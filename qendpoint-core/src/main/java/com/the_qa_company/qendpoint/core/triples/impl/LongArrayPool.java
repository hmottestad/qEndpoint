package com.the_qa_company.qendpoint.core.triples.impl;

import java.util.ArrayDeque;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

final class LongArrayPool {
	private static final int ARRAY_HEADER_BYTES = 16;
	private static final String ENABLED_PROPERTY = "qendpoint.longArrayPool.enabled";
	private static final String CONCURRENCY_PROPERTY = "qendpoint.longArrayPool.concurrency";
	private static final int DEFAULT_CONCURRENCY = Math.max(1, Runtime.getRuntime().availableProcessors());
	private static final long MAX_POOL_BYTES = Math.max(0L, Runtime.getRuntime().maxMemory() / 5L);
	private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
	private static volatile PoolGroup poolGroup = new PoolGroup(readConcurrency());

	private LongArrayPool() {
	}

	static void setEnabled(boolean enabled) {
		if (LongArrayPool.enabled == enabled) {
			return;
		}
		LongArrayPool.enabled = enabled;
		if (!enabled) {
			clearPool();
		}
	}

	static void setConcurrency(int concurrency) {
		int normalized = normalizeConcurrency(concurrency);
		PoolGroup current = poolGroup;
		if (current.concurrency == normalized) {
			return;
		}
		poolGroup = new PoolGroup(normalized);
	}

	static long[] borrow(int minLength) {
		if (minLength <= 0) {
			return new long[0];
		}
		int length = Math.max(1, minLength);
		if (!enabled) {
			return new long[length];
		}
		return poolGroup.poolForThread().borrow(length);
	}

	static void release(long[] array) {
		if (!enabled) {
			return;
		}
		if (array == null || array.length == 0) {
			return;
		}
		poolGroup.poolForThread().release(array);
	}

	private static long estimateBytes(int length) {
		return ARRAY_HEADER_BYTES + (long) length * Long.BYTES;
	}

	private static void clearPool() {
		poolGroup.clear();
	}

	private static int readConcurrency() {
		String raw = System.getProperty(CONCURRENCY_PROPERTY);
		if (raw == null) {
			return DEFAULT_CONCURRENCY;
		}
		try {
			return normalizeConcurrency(Integer.parseInt(raw.trim()));
		} catch (NumberFormatException ignored) {
			return DEFAULT_CONCURRENCY;
		}
	}

	private static int normalizeConcurrency(int concurrency) {
		return Math.max(1, concurrency);
	}

	private static final class PoolGroup {
		private final SubPool[] pools;
		private final int concurrency;

		private PoolGroup(int concurrency) {
			this.concurrency = concurrency;
			long maxPoolBytes = maxPoolBytesPerPool(concurrency);
			this.pools = new SubPool[concurrency];
			for (int i = 0; i < concurrency; i++) {
				pools[i] = new SubPool(maxPoolBytes);
			}
		}

		private SubPool poolForThread() {
			int index = Math.floorMod(Thread.currentThread().getId(), concurrency);
			return pools[index];
		}

		private void clear() {
			for (SubPool pool : pools) {
				pool.clear();
			}
		}
	}

	private static long maxPoolBytesPerPool(int concurrency) {
		if (MAX_POOL_BYTES <= 0L) {
			return 0L;
		}
		return Math.max(0L, MAX_POOL_BYTES / concurrency);
	}

	private static final class SubPool {
		private final StampedLock lock = new StampedLock();
		private final NavigableMap<Integer, ArrayDeque<long[]>> pool = new TreeMap<>();
		private final long maxPoolBytes;
		private long pooledBytes = 0L;

		private SubPool(long maxPoolBytes) {
			this.maxPoolBytes = maxPoolBytes;
		}

		private long[] borrow(int length) {
			long stamp = lock.readLock();
			try {
				var entry = pool.ceilingEntry(length);
				if (entry == null || entry.getValue().isEmpty()) {
					return new long[length];
				}
			} finally {
				lock.unlockRead(stamp);
			}

			long writeStamp = lock.writeLock();
			try {
				var entry = pool.ceilingEntry(length);
				if (entry != null) {
					ArrayDeque<long[]> deque = entry.getValue();
					long[] array = deque.pollFirst();
					if (deque.isEmpty()) {
						pool.remove(entry.getKey());
					}
					if (array != null) {
						pooledBytes -= estimateBytes(array.length);
						return array;
					}
				}
			} finally {
				lock.unlockWrite(writeStamp);
			}
			return new long[length];
		}

		private void release(long[] array) {
			long bytes = estimateBytes(array.length);
			if (bytes <= 0L || maxPoolBytes <= 0L || bytes > maxPoolBytes) {
				return;
			}
			long stamp = lock.writeLock();
			try {
				while (pooledBytes + bytes > maxPoolBytes && !pool.isEmpty()) {
					var entry = pool.lastEntry();
					if (entry == null) {
						break;
					}
					ArrayDeque<long[]> deque = entry.getValue();
					long[] removed = deque.pollFirst();
					if (removed != null) {
						pooledBytes -= estimateBytes(removed.length);
					}
					if (deque.isEmpty()) {
						pool.remove(entry.getKey());
					}
				}
				if (pooledBytes + bytes > maxPoolBytes) {
					return;
				}
				pool.computeIfAbsent(array.length, k -> new ArrayDeque<>()).addFirst(array);
				pooledBytes += bytes;
			} finally {
				lock.unlockWrite(stamp);
			}
		}

		private void clear() {
			long stamp = lock.writeLock();
			try {
				pool.clear();
				pooledBytes = 0L;
			} finally {
				lock.unlockWrite(stamp);
			}
		}
	}
}
