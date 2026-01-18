package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * K-way merge using a loser tree, optimized for simple leaf replay. Exhausted
 * leaves are treated as +infinity via null values. Preserves stability on
 * comparator ties by leaf index (source order in the input list).
 */
public final class FastLoserTreeMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator) {
		return new FastLoserTreeMergeExceptionIterator<>(sources, comparator);
	}

	private final List<? extends ExceptionIterator<T, E>> sources;
	private final Comparator<? super T> comparator;
	private final long size;

	private boolean initialized;

	private int leafStart;
	private ExceptionIterator<T, E>[] iters;
	private int[] tree;
	private Object[] values;

	private int pendingAdvanceLeaf = -1;
	private int pendingReturnLeaf = -1;
	private T next;

	public FastLoserTreeMergeExceptionIterator(List<? extends ExceptionIterator<T, E>> sources,
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

		if (tree.length == 0) {
			return false;
		}

		if (pendingAdvanceLeaf != -1) {
			advanceLeaf(pendingAdvanceLeaf);
			replay(pendingAdvanceLeaf);
			pendingAdvanceLeaf = -1;
		}

		int winnerLeaf = tree[0];
		if (winnerLeaf == -1 || values[winnerLeaf] == null) {
			return false;
		}

		pendingReturnLeaf = winnerLeaf;
		@SuppressWarnings("unchecked")
		T v = (T) values[winnerLeaf];
		next = v;
		return true;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}
		T v = next;
		next = null;
		pendingAdvanceLeaf = pendingReturnLeaf;
		pendingReturnLeaf = -1;
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

		int k = sources.size();
		leafStart = k;

		if (k == 0) {
			iters = (ExceptionIterator<T, E>[]) new ExceptionIterator[0];
			tree = new int[0];
			values = new Object[0];
			return;
		}

		iters = (ExceptionIterator<T, E>[]) new ExceptionIterator[k];
		tree = new int[k * 2];
		values = new Object[k * 2];

		for (int i = 0; i < k; i++) {
			ExceptionIterator<T, E> it = sources.get(i);
			iters[i] = it;

			int leaf = leafStart + i;
			if (it == null || !it.hasNext()) {
				values[leaf] = null;
			} else {
				values[leaf] = it.next();
			}
		}

		buildTree();
	}

	private void buildTree() {
		int[] winners = new int[tree.length];
		for (int i = leafStart; i < tree.length; i++) {
			winners[i] = i;
		}

		for (int i = tree.length - 2; i > 0; i -= 2) {
			int a = winners[i];
			int b = winners[i + 1];

			int winner;
			int loser;
			if (less(a, b)) {
				winner = a;
				loser = b;
			} else {
				winner = b;
				loser = a;
			}

			int p = i >>> 1;
			tree[p] = loser;
			winners[p] = winner;
		}

		tree[0] = winners[1];
	}

	private void replay(int leaf) {
		for (int node = leaf >>> 1; node != 0; node >>>= 1) {
			int loser = tree[node];
			if (less(loser, leaf)) {
				tree[node] = leaf;
				leaf = loser;
			}
		}
		tree[0] = leaf;
	}

	private void advanceLeaf(int leaf) throws E {
		int idx = leaf - leafStart;
		ExceptionIterator<T, E> it = iters[idx];
		if (it != null && it.hasNext()) {
			values[leaf] = it.next();
		} else {
			values[leaf] = null;
		}
	}

	@SuppressWarnings("unchecked")
	private boolean less(int a, int b) {
		Object va = values[a];
		Object vb = values[b];

		if (va == null) {
			return false;
		}
		if (vb == null) {
			return true;
		}

		int c = comparator.compare((T) va, (T) vb);
		if (c != 0) {
			return c < 0;
		}
		return a < b;
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
