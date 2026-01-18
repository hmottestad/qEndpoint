package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressTripleMapper;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.TripleCompressionResult;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;
import com.the_qa_company.qendpoint.core.util.concurrent.KWayMerger;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MapCompressTripleMergerTripleCountTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void mergeToFilePullReportsUniqueTripleCount_singleChunk() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			CloseSuppressPath mapperDir = root.resolve("mapper");
			mapperDir.mkdirs();
			CompressTripleMapper mapper = new IdentityCompressTripleMapper(mapperDir);
			try {
				MapCompressTripleMerger merger = new MapCompressTripleMerger(root.resolve("work"), mapper,
						MultiThreadListener.ignore(), TripleComponentOrder.SPO, 256, 1024, 4, 0);

				ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier = chunkSupplier(
						List.of(List.of(new TripleID(1, 1, 1), new TripleID(1, 1, 1), new TripleID(2, 2, 2))));

				try (TripleCompressionResult result = merger.mergePull(2, CompressionResult.COMPRESSION_MODE_COMPLETE,
						chunkSupplier)) {
					TempTriples triples = result.getTriples();
					long actual = count(triples.searchAll());

					assertEquals("unique triples read back", 2L, actual);
					assertEquals("result should report unique triple count", 2L, result.getTripleCount());
				}
			} catch (KWayMerger.KWayMergerException e) {
				throw e;
			} finally {
				mapper.delete();
			}
		}
	}

	@Test
	public void mergeToFilePullReportsUniqueTripleCount_acrossChunks() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			CloseSuppressPath mapperDir = root.resolve("mapper");
			mapperDir.mkdirs();
			CompressTripleMapper mapper = new IdentityCompressTripleMapper(mapperDir);
			try {
				MapCompressTripleMerger merger = new MapCompressTripleMerger(root.resolve("work"), mapper,
						MultiThreadListener.ignore(), TripleComponentOrder.SPO, 256, 1024, 4, 0);

				ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier = chunkSupplier(
						List.of(List.of(new TripleID(1, 1, 1), new TripleID(2, 2, 2)),
								List.of(new TripleID(2, 2, 2), new TripleID(3, 3, 3))));

				try (TripleCompressionResult result = merger.mergePull(2, CompressionResult.COMPRESSION_MODE_COMPLETE,
						chunkSupplier)) {
					TempTriples triples = result.getTriples();
					long actual = count(triples.searchAll());

					assertEquals("unique triples read back", 3L, actual);
					assertEquals("result should report unique triple count", 3L, result.getTripleCount());
				}
			} finally {
				mapper.delete();
			}
		}
	}

	private static long count(IteratorTripleID it) {
		long count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		return count;
	}

	private static ExceptionSupplier<SizedSupplier<TripleID>, IOException> chunkSupplier(List<List<TripleID>> chunks) {
		AtomicInteger index = new AtomicInteger();
		return () -> {
			int i = index.getAndIncrement();
			if (i >= chunks.size()) {
				return null;
			}
			return sizedSupplier(chunks.get(i));
		};
	}

	private static SizedSupplier<TripleID> sizedSupplier(List<TripleID> triples) {
		Iterator<TripleID> it = triples.iterator();
		return new SizedSupplier<>() {
			@Override
			public long getSize() {
				return triples.size();
			}

			@Override
			public TripleID get() {
				return it.hasNext() ? it.next() : null;
			}
		};
	}

	private static final class IdentityCompressTripleMapper extends CompressTripleMapper {
		IdentityCompressTripleMapper(CloseSuppressPath location) {
			super(location, 16, 1024, false, 0);
		}

		@Override
		public long extractSubject(long id) {
			return id;
		}

		@Override
		public long extractPredicate(long id) {
			return id;
		}

		@Override
		public long extractObjects(long id) {
			return id;
		}

		@Override
		public long extractGraph(long id) {
			return id;
		}
	}
}
