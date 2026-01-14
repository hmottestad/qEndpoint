package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressNodeReader;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SectionCompressorMergeTest {

	@Test
	public void mergeChunksProducesSortedSubjectSection() throws Exception {
		try (CloseSuppressPath tmp = CloseSuppressPath.of(Files.createTempDirectory("SectionCompressorMergeTest"))) {
			tmp.closeWithDeleteRecurse();

			MultiThreadListener listener = MultiThreadListener.ignore();
			int bufferSize = 16 * 1024;
			long chunkSize = 1024L * 1024L;
			int k = 2;

			SectionCompressor compressor = new SectionCompressor(tmp.resolve("base"), listener, bufferSize, chunkSize,
					k, false, false, CompressionType.NONE);

			CloseSuppressPath chunk1 = tmp.resolve("chunk1");
			CloseSuppressPath chunk2 = tmp.resolve("chunk2");
			CloseSuppressPath merged = tmp.resolve("merged");

			compressor.createChunk(supplierOf(Arrays.asList(new TripleString("b", "p1", "o1"),
					new TripleString("a", "p2", "o2"), new TripleString("d", "p3", "o3"))), chunk1);
			compressor.createChunk(supplierOf(Arrays.asList(new TripleString("c", "p4", "o4"),
					new TripleString("a", "p5", "o5"), new TripleString("e", "p6", "o6"))), chunk2);

			compressor.mergeChunks(List.of(chunk1, chunk2), merged);

			List<String> subjects = readNodes(merged.resolve("subject"));
			assertEquals(Arrays.asList("a", "a", "b", "c", "d", "e"), subjects);

			for (int i = 1; i < subjects.size(); i++) {
				assertTrue(subjects.get(i - 1).compareTo(subjects.get(i)) <= 0);
			}
		}
	}

	private static SizedSupplier<TripleString> supplierOf(List<TripleString> triples) {
		Iterator<TripleString> iterator = triples.iterator();
		return new SizedSupplier<>() {
			@Override
			public TripleString get() {
				return iterator.hasNext() ? iterator.next() : null;
			}

			@Override
			public long getSize() {
				return 0;
			}
		};
	}

	private static List<String> readNodes(Path sectionFile) throws Exception {
		List<String> nodes = new ArrayList<>();
		try (InputStream inputStream = Files.newInputStream(sectionFile);
				CompressNodeReader reader = new CompressNodeReader(inputStream)) {
			while (reader.hasNext()) {
				nodes.add(reader.next().getNode().toString());
			}
			reader.checkComplete();
		}
		return nodes;
	}
}
