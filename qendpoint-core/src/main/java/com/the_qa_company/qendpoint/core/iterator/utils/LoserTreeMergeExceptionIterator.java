package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * K-way merge over {@link ExceptionIterator}s using a loser tree.
 * <p>
 * Important: some qEndpoint readers reuse a single mutable object internally.
 * This iterator never advances a cursor before the previously returned element
 * has been handed to the caller.
 */
public class LoserTreeMergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	public static <T, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources, Comparator<? super T> comparator) {
		return new LoserTreeMergeExceptionIterator<>(sources, comparator);
	}

	public static <T extends Comparable<? super T>, E extends Exception> ExceptionIterator<T, E> merge(
			List<? extends ExceptionIterator<T, E>> sources) {
		return merge(sources, Comparable::compareTo);
	}

	private final List<? extends ExceptionIterator<T, E>> sources;
	private final Comparator<? super T> comparator;
	private final long size;

	private boolean initialized;

	private int leafStart;
	private ExceptionIterator<T, E>[] iterators;
	private int[] tree;
	private Object[] values;

	private int pendingAdvanceLeaf = -1;
	private int pendingReturnLeaf = -1;
	private T next;

	public LoserTreeMergeExceptionIterator(List<? extends ExceptionIterator<T, E>> sources,
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
			if (advanceLeaf(pendingAdvanceLeaf)) {
				replayGames(pendingAdvanceLeaf);
			} else {
				sequenceEnded(pendingAdvanceLeaf);
			}
			pendingAdvanceLeaf = -1;
		}

		int winnerLeaf = tree[0];
		if (winnerLeaf == -1 || tree[winnerLeaf] == -1) {
			return false;
		}

		pendingReturnLeaf = winnerLeaf;
		// noinspection unchecked
		next = (T) values[winnerLeaf];
		return true;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}

		T value = next;
		next = null;
		pendingAdvanceLeaf = pendingReturnLeaf;
		pendingReturnLeaf = -1;
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

		leafStart = sources.size();
		if (leafStart == 0) {
			// Ensure we never need to null-check fields later.
			iterators = emptyIterators();
			tree = new int[0];
			values = new Object[0];
			return;
		}

		iterators = emptyIterators(leafStart);
		tree = new int[leafStart * 2];
		values = new Object[leafStart * 2];

		for (int i = 0; i < leafStart; i++) {
			ExceptionIterator<T, E> iterator = sources.get(i);
			iterators[i] = iterator;
			int leaf = leafStart + i;
			if (iterator == null || !iterator.hasNext()) {
				tree[leaf] = -1;
				values[leaf] = null;
				continue;
			}

			tree[leaf] = leaf;
			values[leaf] = iterator.next();
		}

		initializeTree();
	}

	@SuppressWarnings("unchecked")
	private ExceptionIterator<T, E>[] emptyIterators() {
		return (ExceptionIterator<T, E>[]) new ExceptionIterator[0];
	}

	@SuppressWarnings("unchecked")
	private ExceptionIterator<T, E>[] emptyIterators(int size) {
		return (ExceptionIterator<T, E>[]) new ExceptionIterator[size];
	}

	private void initializeTree() {
		int[] winners = new int[tree.length];
		// Initialize leaf nodes as winners to start.
		for (int i = leafStart; i < tree.length; i++) {
			winners[i] = i;
		}
		for (int i = tree.length - 2; i > 0; i -= 2) {
			int a = winners[i];
			int b = winners[i + 1];
			int loser;
			int winner;
			if (less(a, b)) {
				winner = a;
				loser = b;
			} else {
				winner = b;
				loser = a;
			}

			int p = i >>> 1;
			tree[p] = loser;
			values[p] = values[loser];
			winners[p] = winner;
		}
		tree[0] = winners[1];
		values[0] = values[tree[0]];
	}

	private void replayGames(int leaf) {
		for (int node = leaf >>> 1; node != 0; node >>>= 1) {
			int loser = tree[node];
			if (!less(loser, leaf)) {
				continue;
			}

			int oldWinner = leaf;
			leaf = loser;
			tree[node] = oldWinner;
			values[node] = values[oldWinner];
		}

		tree[0] = leaf;
		values[0] = values[leaf];
	}

	/**
	 * @return {@code true} if the leaf is still active after advancing,
	 *         {@code false} if it is exhausted.
	 */
	private boolean advanceLeaf(int leaf) throws E {
		int sourceIndex = leaf - leafStart;
		ExceptionIterator<T, E> iterator = iterators[sourceIndex];
		if (iterator != null && iterator.hasNext()) {
			tree[leaf] = leaf;
			values[leaf] = iterator.next();
			return true;
		}

		tree[leaf] = -1;
		values[leaf] = null;
		return false;
	}

	private void sequenceEnded(int leaf) {
		int node = leaf >>> 1;
		while (node != 0 && tree[tree[node]] == -1) {
			node >>>= 1;
		}
		if (node == 0) {
			tree[0] = leaf;
			values[0] = null;
			return;
		}

		int winner = tree[node];
		tree[node] = leaf;
		values[node] = null;
		replayGames(winner);
	}

	@SuppressWarnings("unchecked")
	private boolean less(int a, int b) {
		boolean aActive = tree[a] != -1;
		boolean bActive = tree[b] != -1;

		if (!aActive) {
			return false;
		}
		if (!bActive) {
			return true;
		}

		int compare = comparator.compare((T) values[a], (T) values[b]);
		if (compare != 0) {
			return compare < 0;
		}
		// Stable tie-break: preserve original source order.
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
