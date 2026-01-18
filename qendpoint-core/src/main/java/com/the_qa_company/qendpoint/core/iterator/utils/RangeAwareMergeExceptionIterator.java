package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Range-aware k-way merge. If iterators expose (min,max) key ranges, this
 * partitions sources into disjoint groups and concatenates groups. Only
 * overlapping groups are merged (2-way or loser tree). If no usable ranges are
 * available, it falls back to a fast loser-tree merge.
 */
public final class RangeAwareMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	/**
	 * Optional interface your iterators can implement to expose range metadata.
	 */
	public interface RangedExceptionIterator<T, E extends Exception> extends ExceptionIterator<T, E> {
		KeyRange<T> keyRange() throws E;
	}

	/**
	 * Simple range container. maxInclusive == null means unknown / +infinity.
	 */
	public static final class KeyRange<T> {
		public final T minInclusive;
		public final T maxInclusive;

		public KeyRange(T minInclusive, T maxInclusive) {
			this.minInclusive = Objects.requireNonNull(minInclusive, "minInclusive");
			this.maxInclusive = maxInclusive;
		}
	}

	/**
	 * Pluggable provider for range metadata. Return null if unknown.
	 */
	@FunctionalInterface
	public interface RangeProvider<T, E extends Exception> {
		KeyRange<T> rangeOf(ExceptionIterator<T, E> it, int sourceIndex) throws E;

		static <T, E extends Exception> RangeProvider<T, E> fromRangedIterators() {
			return (it, idx) -> {
				if (it instanceof RangedExceptionIterator<?, ?> r) {
					@SuppressWarnings("unchecked")
					RangedExceptionIterator<T, E> cast = (RangedExceptionIterator<T, E>) r;
					return cast.keyRange();
				}
				return null;
			};
		}

		static <T, E extends Exception> RangeProvider<T, E> none() {
			return (it, idx) -> null;
		}
	}

	/**
	 * Wrap any iterator with a fixed known range without modifying its class.
	 */
	public static <T, E extends Exception> ExceptionIterator<T, E> ranged(ExceptionIterator<T, E> delegate,
			T minInclusive, T maxInclusive) {
		return new FixedRangeIterator<>(delegate, new KeyRange<>(minInclusive, maxInclusive));
	}

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator) {
		return merge(sources, comparator, RangeProvider.fromRangedIterators());
	}

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator,
			RangeProvider<T, E> rangeProvider) {
		return new RangeAwareMergeExceptionIterator<>(sources, comparator, rangeProvider);
	}

	// --------------------------------------------------------------------------------------------

	private final List<? extends ExceptionIterator<T, E>> sources;
	private final Comparator<? super T> comparator;
	private final RangeProvider<T, E> rangeProvider;
	private final long size;

	private boolean initialized;
	private ExceptionIterator<T, E>[] parts;
	private int partIndex;
	private T next;

	private RangeAwareMergeExceptionIterator(List<? extends ExceptionIterator<T, E>> sources,
			Comparator<? super T> comparator, RangeProvider<T, E> rangeProvider) {
		this.sources = sources == null ? List.of() : sources;
		this.comparator = Objects.requireNonNull(comparator, "comparator");
		this.rangeProvider = rangeProvider == null ? RangeProvider.none() : rangeProvider;
		this.size = computeSize(this.sources);
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}
		initializeIfNeeded();

		while (partIndex < parts.length) {
			ExceptionIterator<T, E> cur = parts[partIndex];
			if (cur.hasNext()) {
				next = cur.next();
				return true;
			}
			partIndex++;
		}
		return false;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}
		T v = next;
		next = null;
		return v;
	}

	@Override
	public long getSize() {
		return size;
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private void initializeIfNeeded() throws E {
		if (initialized) {
			return;
		}
		initialized = true;

		if (sources.isEmpty()) {
			parts = (ExceptionIterator<T, E>[]) new ExceptionIterator[0];
			return;
		}

		ArrayList<SourceRef<T, E>> refs = new ArrayList<>(sources.size());
		boolean anyMaxKnown = false;

		for (int i = 0; i < sources.size(); i++) {
			ExceptionIterator<T, E> it = sources.get(i);
			if (it == null) {
				continue;
			}

			KeyRange<T> range = rangeProvider.rangeOf(it, i);
			T min = range == null ? null : range.minInclusive;
			T max = range == null ? null : range.maxInclusive;

			if (min == null) {
				if (!it.hasNext()) {
					continue;
				}
				T first = it.next();
				min = first;
				it = new PrefetchedIterator<>(first, it);
			}

			if (max != null) {
				anyMaxKnown = true;
			}

			refs.add(new SourceRef<>(i, it, min, max));
		}

		if (refs.isEmpty()) {
			parts = (ExceptionIterator<T, E>[]) new ExceptionIterator[0];
			return;
		}

		if (!anyMaxKnown) {
			ArrayList<ExceptionIterator<T, E>> all = new ArrayList<>(refs.size());
			refs.sort((a, b) -> Integer.compare(a.sourceIndex, b.sourceIndex));
			for (SourceRef<T, E> r : refs) {
				all.add(r.it);
			}
			parts = (ExceptionIterator<T, E>[]) new ExceptionIterator[] {
					FastLoserTreeMergeExceptionIterator.merge(all, comparator) };
			return;
		}

		refs.sort((a, b) -> {
			int c = comparator.compare(a.min, b.min);
			if (c != 0) {
				return c;
			}
			return Integer.compare(a.sourceIndex, b.sourceIndex);
		});

		ArrayList<ArrayList<SourceRef<T, E>>> groups = new ArrayList<>();
		ArrayList<SourceRef<T, E>> current = new ArrayList<>();
		current.add(refs.get(0));
		T groupMax = refs.get(0).max;

		for (int i = 1; i < refs.size(); i++) {
			SourceRef<T, E> s = refs.get(i);
			boolean disjoint = groupMax != null && comparator.compare(groupMax, s.min) < 0;

			if (disjoint) {
				groups.add(current);
				current = new ArrayList<>();
				current.add(s);
				groupMax = s.max;
			} else {
				current.add(s);
				groupMax = maxKey(groupMax, s.max);
			}
		}
		groups.add(current);

		ArrayList<ExceptionIterator<T, E>> planned = new ArrayList<>(groups.size());
		for (ArrayList<SourceRef<T, E>> group : groups) {
			if (group.isEmpty()) {
				continue;
			}
			if (group.size() == 1) {
				planned.add(group.get(0).it);
				continue;
			}

			group.sort((a, b) -> Integer.compare(a.sourceIndex, b.sourceIndex));
			if (group.size() == 2) {
				planned.add(new StableMergeExceptionIterator<>(group.get(0).it, group.get(1).it, comparator));
			} else {
				ArrayList<ExceptionIterator<T, E>> its = new ArrayList<>(group.size());
				for (SourceRef<T, E> r : group) {
					its.add(r.it);
				}
				planned.add(FastLoserTreeMergeExceptionIterator.merge(its, comparator));
			}
		}

		parts = planned.toArray((ExceptionIterator<T, E>[]) new ExceptionIterator[0]);
	}

	private T maxKey(T a, T b) {
		if (a == null || b == null) {
			return null;
		}
		return comparator.compare(a, b) >= 0 ? a : b;
	}

	private static final class SourceRef<T, E extends Exception> {
		final int sourceIndex;
		final ExceptionIterator<T, E> it;
		final T min;
		final T max;

		SourceRef(int sourceIndex, ExceptionIterator<T, E> it, T min, T max) {
			this.sourceIndex = sourceIndex;
			this.it = it;
			this.min = min;
			this.max = max;
		}
	}

	private static final class FixedRangeIterator<T, E extends Exception> implements RangedExceptionIterator<T, E> {
		private final ExceptionIterator<T, E> delegate;
		private final KeyRange<T> range;

		FixedRangeIterator(ExceptionIterator<T, E> delegate, KeyRange<T> range) {
			this.delegate = Objects.requireNonNull(delegate, "delegate");
			this.range = Objects.requireNonNull(range, "range");
		}

		@Override
		public KeyRange<T> keyRange() {
			return range;
		}

		@Override
		public boolean hasNext() throws E {
			return delegate.hasNext();
		}

		@Override
		public T next() throws E {
			return delegate.next();
		}

		@Override
		public long getSize() {
			return delegate.getSize();
		}
	}

	private static final class PrefetchedIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
		private final ExceptionIterator<T, E> delegate;
		private T prefetched;

		PrefetchedIterator(T first, ExceptionIterator<T, E> delegate) {
			this.prefetched = first;
			this.delegate = delegate;
		}

		@Override
		public boolean hasNext() throws E {
			return prefetched != null || delegate.hasNext();
		}

		@Override
		public T next() throws E {
			if (prefetched != null) {
				T v = prefetched;
				prefetched = null;
				return v;
			}
			return delegate.next();
		}

		@Override
		public long getSize() {
			return delegate.getSize();
		}
	}

	/** Stable 2-way merge (left-biased on ties). */
	private static final class StableMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {
		private final ExceptionIterator<T, E> a;
		private final ExceptionIterator<T, E> b;
		private final Comparator<? super T> comp;

		private T next;
		private T headA;
		private T headB;

		StableMergeExceptionIterator(ExceptionIterator<T, E> a, ExceptionIterator<T, E> b, Comparator<? super T> comp) {
			this.a = a;
			this.b = b;
			this.comp = comp;
		}

		@Override
		public boolean hasNext() throws E {
			if (next != null) {
				return true;
			}

			if (headA == null && a.hasNext()) {
				headA = a.next();
			}
			if (headB == null && b.hasNext()) {
				headB = b.next();
			}

			if (headA != null && headB != null) {
				if (comp.compare(headA, headB) <= 0) {
					next = headA;
					headA = null;
				} else {
					next = headB;
					headB = null;
				}
				return true;
			}

			if (headA != null) {
				next = headA;
				headA = null;
				return true;
			}
			if (headB != null) {
				next = headB;
				headB = null;
				return true;
			}
			return false;
		}

		@Override
		public T next() throws E {
			if (!hasNext()) {
				return null;
			}
			T v = next;
			next = null;
			return v;
		}

		@Override
		public long getSize() {
			long sa = a.getSize();
			long sb = b.getSize();
			if (sa == -1 || sb == -1) {
				return -1;
			}
			return sa + sb;
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
