package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MapCompressTripleMergerRangeTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void mergeChunksRegistersRangeMetadata() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			CloseSuppressPath mapperDir = root.resolve("mapper");
			mapperDir.mkdirs();

			CompressTripleMapper mapper = new CompressTripleMapper(mapperDir, 10, 1024, false, 0);
			MapCompressTripleMerger merger = new MapCompressTripleMerger(root, mapper, MultiThreadListener.ignore(),
					TripleComponentOrder.SPO, 1024, 1024, 2, 0);

			CloseSuppressPath input1 = root.resolve("chunk1");
			CloseSuppressPath input2 = root.resolve("chunk2");
			CloseSuppressPath output = root.resolve("merged");

			try {
				writeTriples(input1, List.of(new TripleID(1, 1, 1), new TripleID(2, 2, 2)));
				writeTriples(input2, List.of(new TripleID(4, 4, 4), new TripleID(5, 5, 5)));

				merger.mergeChunks(List.of(input1, input2), output);

				Constructor<CompressTripleReader> ctor = CompressTripleReader.class
						.getConstructor(CloseSuppressPath.class, int.class);
				try (CompressTripleReader reader = ctor.newInstance(output, 1024)) {
					assertTrue(reader instanceof RangedExceptionIterator);
					@SuppressWarnings("rawtypes")
					RangedExceptionIterator ranged = (RangedExceptionIterator) reader;
					@SuppressWarnings("unchecked")
					KeyRange<TripleID> range = (KeyRange<TripleID>) ranged.keyRange();
					assertNotNull(range);
					assertEquals(new TripleID(1, 1, 1), range.minInclusive);
					assertEquals(new TripleID(5, 5, 5), range.maxInclusive);
				}
			} finally {
				output.close();
				CompressTripleRange.deleteRangeIfExists(output);
				mapper.delete();
			}
			root.closeWithDeleteRecurse();
		}
	}

	private static void writeTriples(CloseSuppressPath path, List<TripleID> triples) throws Exception {
		try (CompressTripleWriter writer = new CompressTripleWriter(path.openOutputStream(1024), false)) {
			for (TripleID triple : triples) {
				writer.appendTriple(triple);
			}
		}
	}
}
