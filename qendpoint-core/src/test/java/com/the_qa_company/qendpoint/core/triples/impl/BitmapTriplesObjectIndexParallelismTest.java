package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class BitmapTriplesObjectIndexParallelismTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void objectIndexParallelismOptionIsApplied() throws Exception {
		Path root = tempDir.newFolder().toPath();
		Path hdtPath = root.resolve("hdt.hdt");

		try {
			try (HDT hdt = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(2_000L, 42)
					.createFakeHDT(new HDTSpecification())) {
				hdt.saveToHDT(hdtPath.toAbsolutePath().toString(), null);
			}

			HDTOptions options = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED,
					HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY, 2);

			try (HDT indexed = BitmapTriplesTest.loadOrMapIndexed(hdtPath, options, false)) {
				BitmapTriples triples = (BitmapTriples) indexed.getTriples();
				int parallelism = readObjectIndexParallelism(triples);
				assertEquals("object index parallelism", 2, parallelism);
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}

	private static int readObjectIndexParallelism(BitmapTriples triples) throws Exception {
		Field field = BitmapTriples.class.getDeclaredField("objectIndexParallelism");
		field.setAccessible(true);
		return (int) field.get(triples);
	}
}
