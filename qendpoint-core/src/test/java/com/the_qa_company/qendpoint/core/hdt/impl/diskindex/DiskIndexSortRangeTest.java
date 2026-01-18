package com.the_qa_company.qendpoint.core.hdt.impl.diskindex;

import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.compress.Pair;
import com.the_qa_company.qendpoint.core.util.io.compress.PairReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DiskIndexSortRangeTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void createChunkWritesRangeSidecar() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			Comparator<Pair> comparator = Comparator.<Pair>comparingLong(p -> p.predicatePosition)
					.thenComparingLong(p -> p.object).thenComparingLong(p -> p.predicate);
			DiskIndexSort sorter = new DiskIndexSort(root, MultiThreadListener.ignore(), 1024, 1024, 2, comparator);

			Pair p1 = pair(5, 1, 3);
			Pair p2 = pair(2, 9, 7);
			Pair p3 = pair(5, 1, 4);
			List<Pair> pairs = List.of(p1, p2, p3);

			CloseSuppressPath output = root.resolve("chunk");
			try {
				sorter.createChunk(new ListSizedSupplier<>(pairs), output);

				Constructor<?> ctor = PairReader.class.getConstructor(CloseSuppressPath.class, int.class);
				try (PairReader reader = (PairReader) ctor.newInstance(output, 1024)) {
					assertTrue(
							reader instanceof com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator);
					@SuppressWarnings("rawtypes")
					com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator ranged = (com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator) reader;
					@SuppressWarnings("unchecked")
					com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange<Pair> range = (com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange<Pair>) ranged
							.keyRange();
					assertNotNull(range);

					Pair min = range.minInclusive;
					Pair max = range.maxInclusive;
					assertPairEquals(p2, min);
					assertPairEquals(p3, max);
				}
			} finally {
				output.close();
				Files.deleteIfExists(output.resolveSibling(output.getFileName().toString() + ".range"));
			}
			root.closeWithDeleteRecurse();
		}
	}

	private static Pair pair(long predicatePosition, long object, long predicate) {
		Pair pair = new Pair();
		pair.setAll(predicatePosition, object, predicate);
		return pair;
	}

	private static void assertPairEquals(Pair expected, Pair actual) {
		assertEquals(expected.predicatePosition, actual.predicatePosition);
		assertEquals(expected.object, actual.object);
		assertEquals(expected.predicate, actual.predicate);
	}

	private static final class ListSizedSupplier<T> implements SizedSupplier<T> {
		private final Iterator<T> iterator;

		private ListSizedSupplier(List<T> items) {
			this.iterator = items.iterator();
		}

		@Override
		public long getSize() {
			return 0;
		}

		@Override
		public T get() {
			return iterator.hasNext() ? iterator.next() : null;
		}
	}
}
