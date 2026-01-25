package com.the_qa_company.qendpoint.core.triples.impl;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

public class BitmapTriplesSortByValueThenPositionTest {
	private static final class Pair {
		private final long value;
		private final long position;

		private Pair(long value, long position) {
			this.value = value;
			this.position = position;
		}
	}

	@Test
	public void sortByValueThenPositionOrdersPairs() throws Exception {
		long[] values = { 5L, 2L, 2L, 1L, 5L, 3L };
		long[] positions = { 9L, 4L, 1L, 7L, 3L, 8L };

		Pair[] expected = new Pair[values.length];
		for (int i = 0; i < values.length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Method method = BitmapTriples.class.getDeclaredMethod("sortByValueThenPosition", long[].class, long[].class,
				int.class, int.class);
		method.setAccessible(true);
		method.invoke(null, values, positions, 0, values.length);

		for (int i = 0; i < values.length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void timSortMergesLongRuns() throws Exception {
		int run = 1024;
		int length = run * 2;
		long[] values = new long[length];
		long[] positions = new long[length];

		for (int i = 0; i < run; i++) {
			values[i] = i;
			positions[i] = i;
		}
		for (int i = 0; i < run; i++) {
			values[run + i] = i;
			positions[run + i] = run + i;
		}

		Pair[] expected = new Pair[length];
		for (int i = 0; i < length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Class<?> timSortClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$LongPairTimSort");
		Method sort = timSortClass.getDeclaredMethod("sort", long[].class, long[].class, int.class, int.class);
		sort.setAccessible(true);
		sort.invoke(null, values, positions, 0, length);

		for (int i = 0; i < length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void parallelQuickSortOrdersPairs() throws Exception {
		int length = 1 << 18;
		long[] values = new long[length];
		long[] positions = new long[length];

		long seed = 1L;
		for (int i = 0; i < length; i++) {
			seed = seed * 6364136223846793005L + 1;
			values[i] = seed & 1023L;
			positions[i] = i;
		}

		Pair[] expected = new Pair[length];
		for (int i = 0; i < length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Method sort = BitmapTriples.class.getDeclaredMethod("parallelQuickSortPairs", long[].class, long[].class,
				int.class, int.class);
		sort.setAccessible(true);
		sort.invoke(null, values, positions, 0, length - 1);

		for (int i = 0; i < length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}

	@Test
	public void estimateAvailableMemoryCachesBetweenRefreshes() throws Exception {
		Method method = BitmapTriples.class.getDeclaredMethod("estimateAvailableMemory");
		method.setAccessible(true);

		java.lang.reflect.Field cacheField = BitmapTriples.class.getDeclaredField("MEMORY_ESTIMATE_CACHE");
		java.lang.reflect.Field counterField = BitmapTriples.class.getDeclaredField("MEMORY_ESTIMATE_CALLS");
		cacheField.setAccessible(true);
		counterField.setAccessible(true);

		long originalCache = cacheField.getLong(null);
		int originalCounter = counterField.getInt(null);

		try {
			long sentinel = Long.MAX_VALUE / 4;
			cacheField.setLong(null, sentinel);
			counterField.setInt(null, 0);

			long cached = ((Long) method.invoke(null)).longValue();
			assertEquals(sentinel, cached);

			cacheField.setLong(null, sentinel);
			counterField.setInt(null, 99);

			long refreshed = ((Long) method.invoke(null)).longValue();
			assertNotEquals(sentinel, refreshed);
		} finally {
			cacheField.setLong(null, originalCache);
			counterField.setInt(null, originalCounter);
		}
	}

	@Test
	public void longArrayPoolReusesArrays() throws Exception {
		Class<?> poolClass = Class.forName("com.the_qa_company.qendpoint.core.triples.impl.LongArrayPool");
		Method borrow = poolClass.getDeclaredMethod("borrow", int.class);
		Method release = poolClass.getDeclaredMethod("release", long[].class);
		borrow.setAccessible(true);
		release.setAccessible(true);

		long[] first = (long[]) borrow.invoke(null, 8);
		release.invoke(null, new Object[] { first });

		long[] second = (long[]) borrow.invoke(null, 4);
		assertSame(first, second);
	}
}
