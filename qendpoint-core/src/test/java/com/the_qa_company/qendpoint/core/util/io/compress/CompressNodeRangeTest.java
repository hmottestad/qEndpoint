package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompressNodeRangeTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void writeCompressedSectionStoresRange() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			CloseSuppressPath output = root.resolve("nodes");
			List<IndexedNode> nodes = List.of(new IndexedNode("alpha", 1), new IndexedNode("beta", 2),
					new IndexedNode("delta", 3));

			Object rangeObj;
			try (OutputStream stream = output.openOutputStream(1024)) {
				Method method = CompressUtil.class.getMethod("writeCompressedSectionWithRange", ExceptionIterator.class,
						long.class, OutputStream.class, ProgressListener.class);
				rangeObj = method.invoke(null, ExceptionIterator.of(nodes.iterator()), (long) nodes.size(), stream,
						ProgressListener.ignore());
			}
			Class<?> rangeWriter = Class
					.forName("com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeRange");
			Method writeRange = rangeWriter.getMethod("writeRange", CloseSuppressPath.class, rangeObj.getClass());
			writeRange.invoke(null, output, rangeObj);

			Constructor<?> ctor = CompressNodeReader.class.getConstructor(CloseSuppressPath.class, int.class);
			try (CompressNodeReader reader = (CompressNodeReader) ctor.newInstance(output, 1024)) {
				assertTrue(
						reader instanceof com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator);
				@SuppressWarnings("rawtypes")
				com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator ranged = (com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.RangedExceptionIterator) reader;
				@SuppressWarnings("unchecked")
				com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange<IndexedNode> keyRange = (com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange<IndexedNode>) ranged
						.keyRange();
				assertNotNull(keyRange);
				assertEquals(new CompactString("alpha"), keyRange.minInclusive.getNode());
				assertEquals(new CompactString("delta"), keyRange.maxInclusive.getNode());
			} finally {
				output.close();
				Files.deleteIfExists(output.resolveSibling(output.getFileName().toString() + ".range"));
			}
			root.closeWithDeleteRecurse();
		}
	}
}
