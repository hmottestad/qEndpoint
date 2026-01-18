package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * K-way merge over {@link ExceptionIterator}s using a {@link PriorityQueue}.
 * <p>
 * Important: some qEndpoint readers reuse a single mutable object internally.
 * This iterator never advances a cursor before the previously returned element
 * has been handed to the caller.
 */
public class PriorityQueueMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator) {
		return new PriorityQueueMergeExceptionIterator<>(sources, comparator);
	}

	public static <T extends Comparable<? super T>, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources) {
		return merge(sources, Comparable::compareTo);
	}

	private static final class Cursor<T, E extends Exception> {
		final int index;
		final ExceptionIterator<T, E> iterator;
		T head;

		Cursor(int index, ExceptionIterator<T, E> iterator) {
			this.index = index;
			this.iterator = iterator;
		}

		boolean advance() throws E {
			if (iterator.hasNext()) {
				head = iterator.next();
				return true;
			}
			head = null;
			return false;
		}
	}

	private final List<? extends ExceptionIterator<T, E>> sources;
	private final Comparator<? super T> comparator;
	private final long size;

	private PriorityQueue<Cursor<T, E>> queue;
	private boolean initialized;

	private Cursor<T, E> pendingAdvance;
	private Cursor<T, E> pendingReturn;
	private T next;

	public PriorityQueueMergeExceptionIterator(List<? extends ExceptionIterator<T, E>> sources,
			Comparator<? super T> comparator) {
		this.sources = sources == null ? List.of() : sources;
		this.comparator = Objects.requireNonNull(comparator, "comparator");
		this.size = computeSize(this.sources);
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}
		initializeIfNeeded();

		if (pendingAdvance != null) {
			if (pendingAdvance.advance()) {
				queue.add(pendingAdvance);
			}
			pendingAdvance = null;
		}

		if (queue.isEmpty()) {
			return false;
		}

		pendingReturn = queue.poll();
		next = pendingReturn.head;
		pendingReturn.head = null;
		return true;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}

		T value = next;
		next = null;
		pendingAdvance = pendingReturn;
		pendingReturn = null;
		return value;
	}

	@Override
	public long getSize() {
		return size;
	}

	private void initializeIfNeeded() throws E {
		if (initialized) {
			return;
		}
		initialized = true;

		queue = new PriorityQueue<>(Math.max(1, sources.size()), (a, b) -> {
			int compare = comparator.compare(a.head, b.head);
			if (compare != 0) {
				return compare;
			}
			return Integer.compare(a.index, b.index);
		});

		for (int i = 0; i < sources.size(); i++) {
			ExceptionIterator<T, E> iterator = sources.get(i);
			if (iterator == null) {
				continue;
			}
			Cursor<T, E> cursor = new Cursor<>(i, iterator);
			if (cursor.advance()) {
				queue.add(cursor);
			}
		}
	}

	private static <T, E extends Exception> long computeSize(List<? extends ExceptionIterator<T, E>> sources) {
		long size = 0;
		for (ExceptionIterator<T, E> source : sources) {
			if (source == null) {
				continue;
			}
			long s = source.getSize();
			if (s == -1) {
				return -1;
			}
			size += s;
		}
		return size;
	}
}
