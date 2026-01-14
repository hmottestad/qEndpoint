package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MergeExceptionIteratorLooserTree<T, E extends Exception> implements ExceptionIterator<T, E> {

	public MergeExceptionIteratorLooserTree(ExceptionIterator<T, E> a, ExceptionIterator<T, E> b,
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
		return new MergeExceptionIteratorLooserTree<>(array, Comparable::compareTo, sentinelValue);
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

		return new MergeExceptionIteratorLooserTree<>(iterators, comp, sentinelValue);
	}

	/**
	 * Sentinel value regarded as greater than any real element.
	 */
	private final T sentinelValue;

	/**
	 * Comparator used to order the elements.
	 */
	private final Comparator<? super T> comparator;

	/**
	 * A complete binary array tree – {@code treeSize = 2 * sourceCount}. Index
	 * layout:
	 * <ul>
	 * <li>0: root sentinel node (stores the overall winner)</li>
	 * <li>1..(sourceCount - 1): internal nodes storing sub-losers</li>
	 * <li>sourceCount..(2*sourceCount - 1): leaves for each input iterator</li>
	 * </ul>
	 */
	private final Node<T, E>[] nodes;

	/**
	 * Constructs a new LoserTree from the given input iterators, using the
	 * specified comparator for ordering.
	 *
	 * @param sources       a list of iterators providing sorted elements
	 * @param sentinelValue a value treated as "infinite" once any iterator is
	 *                      exhausted
	 * @param comparator    the ordering used to merge elements
	 * @throws IllegalArgumentException if sources is empty or null
	 * @throws NullPointerException     if sentinelValue or comparator is null
	 */
	@SuppressWarnings("unchecked")
	public MergeExceptionIteratorLooserTree(List<ExceptionIterator<T, E>> sources, Comparator<? super T> comparator,
			T sentinelValue) {
		if (sources == null) {
			throw new IllegalArgumentException("sources must not be null.");
		}
		if (sentinelValue == null) {
			throw new NullPointerException("sentinelValue must not be null.");
		}
		if (comparator == null) {
			throw new NullPointerException("comparator must not be null.");
		}

		this.comparator = comparator;
		this.sentinelValue = sentinelValue;

		int sourceCount = sources.size();
		if (sourceCount == 0) {
			// Create an empty array so isEmpty() just returns true, etc.
			this.nodes = (Node<T, E>[]) new Node[0];
			return;
		}

		// Allocate node array: size = 2 * sourceCount
		this.nodes = (Node<T, E>[]) new Node[sourceCount * 2];
		for (int i = 0; i < nodes.length; i++) {
			nodes[i] = new Node<>();
		}

		// Initialize leaves with their first element (if any)
		for (int i = 0; i < sourceCount; i++) {
			int leafIndex = i + sourceCount;
			nodes[leafIndex].iterator = sources.get(i);
			try {
				advanceLeaf(leafIndex);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		// Build initial winner if at least one iterator is non-empty
		buildInitialWinner();
	}

	/* --------------------------------------------------------------------- */
	/* Iterator<E> implementation */
	/* --------------------------------------------------------------------- */

	@Override
	public boolean hasNext() throws E {
		if (prevWinner == -1) {
			// Advance that winner’s leaf and replay up the tree
			advanceLeaf(prevWinner);
			replayUpToRoot(prevWinner);
			prevWinner = -1;
		}

		return !isEmpty();
	}

	int prevWinner = -1;

	@Override
	public T next() throws E {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		// Index 0 stores the overall winner’s index & value
		int winnerIndex = nodes[0].index;
		T result = nodes[0].value;

		nodes[0] = null;
		prevWinner = winnerIndex;

		// Advance that winner’s leaf and replay up the tree
		advanceLeaf(winnerIndex);
		replayUpToRoot(winnerIndex);

		return result;
	}

	/* --------------------------------------------------------------------- */
	/* Public helpers */
	/* --------------------------------------------------------------------- */

	/**
	 * @return {@code true} if every input iterator is exhausted
	 */
	public boolean isEmpty() {
		boolean isEmpty = nodes.length == 0 || nodes[0].index == -1;
		if (!isEmpty) {
			return nodes[0].value == sentinelValue;
		}
		return isEmpty;
	}

	/**
	 * Return the iterator whose element is currently the winner. (For
	 * diagnostics; mutating that iterator directly will break invariants.)
	 */
	public ExceptionIterator<T, E> winnerIterator() {
		return isEmpty() ? null : nodes[nodes[0].index].iterator;
	}

	/**
	 * @return the minimum element currently at the front of the merge
	 */
	public T peek() {
		return isEmpty() ? null : nodes[0].value;
	}

	/* --------------------------------------------------------------------- */
	/* Internal algorithm */
	/* --------------------------------------------------------------------- */

	/**
	 * Compute the initial winner for the entire tree by recursively comparing
	 * subtrees.
	 */
	private void buildInitialWinner() {
		// If no leaves, do nothing; isEmpty() handles that
		if (nodes.length == 0) {
			return;
		}
		int sourceCount = nodes.length / 2;
		int overallWinner = play(1, sourceCount);
		nodes[0].index = overallWinner;
		nodes[0].value = nodes[overallWinner].value;
	}

	/**
	 * Recursively compute the winner index for the subtree rooted at
	 * {@code position}.
	 *
	 * @param position    current internal node
	 * @param sourceCount number of leaves
	 * @return index of the leaf that wins at this subtree
	 */
	private int play(int position, int sourceCount) {
		// If we're at a leaf, return it immediately
		if (position >= sourceCount) {
			return position;
		}
		int leftWinner = play(position << 1, sourceCount);
		int rightWinner = play((position << 1) + 1, sourceCount);

		// Compare the winners, designating one as the "loser" stored in the
		// internal node
		int winnerIndex;
		int loserIndex;
		if (compareNodes(leftWinner, rightWinner) > 0) {
			winnerIndex = leftWinner;
			loserIndex = rightWinner;
		} else {
			winnerIndex = rightWinner;
			loserIndex = leftWinner;
		}

		Node<T, E> node = nodes[position];
		node.index = loserIndex;
		node.value = nodes[loserIndex].value;

		return winnerIndex;
	}

	/**
	 * Re-run comparisons from {@code leafIndex} up through its parents,
	 * updating internal nodes and eventually the root sentinel (index 0).
	 */
	private void replayUpToRoot(int leafIndex) {
		T currentValue = nodes[leafIndex].value;
		int currentIndex = leafIndex;

		// Move up until we reach the sentinel root at index 0
		for (int parentIndex = parent(leafIndex); parentIndex != 0; parentIndex = parent(parentIndex)) {
			Node<T, E> parentNode = nodes[parentIndex];
			// If the existing stored index is strictly smaller, it remains the
			// winner
			if (compareNodes(parentNode.index, currentIndex) < 0) {
				// do nothing
			} else {
				// Swap winner/loser for this level
				int tmpIdx = parentNode.index;
				parentNode.index = currentIndex;
				currentIndex = tmpIdx;

				T tmpVal = parentNode.value;
				parentNode.value = currentValue;
				currentValue = tmpVal;
			}
		}

		// The root node gets the final winner
		nodes[0].index = currentIndex;
		nodes[0].value = currentValue;
	}

	/**
	 * Compare two leaf node indices by their stored values (exhausted nodes
	 * become largest).
	 */
	private int compareNodes(int leftLeaf, int rightLeaf) {
		T leftVal = nodes[leftLeaf].value;
		T rightVal = nodes[rightLeaf].value;
		if (leftVal == sentinelValue) {
			if (rightVal == sentinelValue) {
				return 0;
			}
			return -1;
		}
		if (rightVal == sentinelValue) {
			return 1;
		}

		return comparator.compare(rightVal, leftVal);
	}

	/**
	 * Advance the leaf at {@code leafIndex} by consuming its next element. If
	 * none remain, set it to the sentinelValue.
	 */
	private void advanceLeaf(int leafIndex) throws E {
		Node<T, E> leaf = nodes[leafIndex];
		if (leaf.iterator != null && leaf.iterator.hasNext()) {
			leaf.value = leaf.iterator.next();
			leaf.index = leafIndex;
		} else {
			leaf.value = sentinelValue;
			leaf.index = -1; // exhausted
		}
	}

	/**
	 * Computes the parent index (integer division by 2).
	 */
	private static int parent(int i) {
		return i >>> 1; // same as i / 2, but faster
	}

	/* --------------------------------------------------------------------- */
	/* Internal node representation */
	/* --------------------------------------------------------------------- */

	private static final class Node<T, E extends Exception> {
		/**
		 * For internal nodes: the "loser" index. For root: winner index. -1 =
		 * exhausted.
		 */
		int index;

		/**
		 * Value associated with {@link #index}.
		 */
		T value;

		/**
		 * Iterator reference, only meaningful for leaves.
		 */
		ExceptionIterator<T, E> iterator;
	}

	@Override
	public long getSize() {
		long size = 0;
		for (Node<T, E> node : nodes) {
			if (node.iterator != null) {
				long s = node.iterator.getSize();
				if (s == -1) {
					return -1;
				}
				size += s;
			}
		}
		return size;
	}

}
