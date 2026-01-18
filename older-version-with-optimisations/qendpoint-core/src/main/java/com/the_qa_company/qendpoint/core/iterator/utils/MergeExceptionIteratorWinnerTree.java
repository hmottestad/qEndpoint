package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeExceptionIteratorWinnerTree<T, E extends Exception> implements ExceptionIterator<T, E> {

	public MergeExceptionIteratorWinnerTree(ExceptionIterator<T, E> a, ExceptionIterator<T, E> b,
			Comparator<? super T> comparator, T sentinelValue) {
		this(Arrays.asList(a, b), comparator, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param length     the number of elements
	 * @param <I>        input of the element
	 * @param <T>        type of the element in the iterator
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int length,
			T sentinelValue) {
		return buildOfTree(itFunction, comp, array, 0, length, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int start, int end,
			T sentinelValue) {
		return buildOfTree(itFunction, comp, Arrays.asList(array), start, end, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start, int end,
			T sentinelValue) {
		return buildOfTree((index, o) -> itFunction.apply(o), comp, array, start, end, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T extends Comparable<T>, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, List<I> array, int start, int end, T sentinelValue) {
		return buildOfTree((index, o) -> itFunction.apply(o), Comparable::compareTo, array, start, end, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array the elements
	 * @param <T>   type of the element
	 * @param <E>   exception returned by the iterator
	 * @return the iterator
	 */
	public static <T extends Comparable<? super T>, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			List<ExceptionIterator<T, E>> array, T sentinelValue) {
		return new MergeExceptionIteratorWinnerTree<>(array, Comparable::compareTo, sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param array      the elements
	 * @param comparator comparator for the merge iterator
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <T, E extends Exception> ExceptionIterator<T, E> buildOfTree(List<ExceptionIterator<T, E>> array,
			Comparator<T> comparator, T sentinelValue) {
		return buildOfTree(Function.identity(), comparator, array, 0, array.size(), sentinelValue);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 *
	 * @param itFunction a function to create an iterator from an element
	 * @param comp       comparator for the merge iterator
	 * @param array      the elements
	 * @param start      the start of the array (inclusive)
	 * @param end        the end of the array (exclusive)
	 * @param <T>        type of the element
	 * @param <E>        exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			BiFunction<Integer, I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start,
			int end, T sentinelValue) {
		int length = end - start;
		if (length <= 0) {
			return ExceptionIterator.empty();
		}
		if (length == 1) {
			return itFunction.apply(start, array.get(start));
		}

		ArrayList<ExceptionIterator<T, E>> iterators = new ArrayList<>(2);
		for (int i = start; i < end; i++) {
			ExceptionIterator<T, E> it = itFunction.apply(i, array.get(i));
			if (it != null) {
				iterators.add(it);
			}
		}

		return new MergeExceptionIteratorWinnerTree<>(iterators, comp, sentinelValue);
	}

	private final ExceptionIterator<T, E>[] runs; // the k input iterators
	private final T[] head; // current head of each run
	private final int[] tree; // winner indexes; tree[1] is root
	private final Comparator<? super T> cmp;
	private final T sentinel;
	private final int k; // number of runs (power of two not required)

	@SuppressWarnings("unchecked")
	public MergeExceptionIteratorWinnerTree(List<ExceptionIterator<T, E>> runs, Comparator<? super T> cmp, T sentinel) {
		this.k = runs.size();
		this.runs = runs.toArray((ExceptionIterator<T, E>[]) new ExceptionIterator[k]);
		this.cmp = cmp;
		this.sentinel = sentinel;

		this.head = (T[]) new Object[k];
		this.tree = new int[2 * k]; // 1-based array for simpler math
		try {
			build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** Fill leaves, then build the tournament bottom-up (O(k)). */
	private void build() throws E {
		// 1. Fetch the first element (or sentinel) for every run.
		for (int i = 0; i < k; i++) {
			head[i] = runs[i].hasNext() ? runs[i].next() : sentinel;
			tree[k + i] = i; // leaf i keeps its run index
		}
		// 2. Play matches bottom-up.
		for (int i = k - 1; i > 0; i--) {
			int left = tree[i << 1];
			int right = tree[(i << 1) | 1];
			tree[i] = smaller(left, right);
		}
	}

	/** Compare two run heads, return the index of the smaller key. */
	private int smaller(int a, int b) {
		if (head[a] == sentinel)
			return b; // a is empty
		if (head[b] == sentinel)
			return a; // b is empty

		return cmp.compare(head[a], head[b]) <= 0 ? a : b;
	}

	int prev = -1;

	@Override
	public boolean hasNext() throws E {
		if (prev != -1) {
			// Advance that run – iterator may reuse the same object instance
			head[prev] = runs[prev].hasNext() ? runs[prev].next() : sentinel;

			// Re-play matches on the path from leaf (k + prev) to root
			int node = (k + prev) >> 1;
			while (node > 0) {
				int left = tree[node << 1];
				int right = tree[(node << 1) | 1];
				tree[node] = smaller(left, right);
				node >>= 1;
			}
			prev = -1; // reset
		}

		return head[tree[1]] != sentinel; // root holds global winner
	}

	@Override
	public T next() throws E {
		if (!hasNext())
			throw new NoSuchElementException();
		int w = tree[1]; // winning run index
		T r = head[w]; // record to return

		prev = w;
		head[w] = null;

		return r;
	}

	@Override
	public long getSize() {
		long size = 0;
		for (var node : runs) {
			if (node != null) {
				long s = node.getSize();
				if (s == -1) {
					return -1;
				}
				size += s;
			}
		}
		return size;
	}

}
