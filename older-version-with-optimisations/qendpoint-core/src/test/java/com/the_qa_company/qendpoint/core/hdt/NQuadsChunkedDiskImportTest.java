package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class NQuadsChunkedDiskImportTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void generateDiskWithSimpleParserUsesPullChunking() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();
		Path nq = root.resolve("data.nq");

		Files.writeString(nq, "<http://ex/s> <http://ex/p> \"o\" <http://ex/g> .\n"
				+ "<http://ex/s2> <http://ex/p2> <http://ex/o2> <http://ex/g2> .\n");

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true", HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("work").toAbsolutePath().toString(), HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				"2", HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, "1");

		try (HDT hdt = HDTManager.generateHDTDisk(nq.toString(), "http://ex/", RDFNotation.NQUAD, spec, null)) {
			assertEquals(2, hdt.getTriples().getNumberOfElements());
		}
	}
}
