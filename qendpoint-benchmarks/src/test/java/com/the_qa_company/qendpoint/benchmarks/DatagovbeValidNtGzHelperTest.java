package com.the_qa_company.qendpoint.benchmarks;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

public class DatagovbeValidNtGzHelperTest {

	@Test
	public void ensureDatagovbeValidNtGz_createsFileWithNTriples() throws Exception {
		Path repoRoot = Files.createTempDirectory("qendpoint-benchmarks");
		Path indexingDir = repoRoot.resolve("indexing");
		Files.createDirectories(indexingDir);

		Path ttlFile = indexingDir.resolve("datagovbe-valid.ttl");
		Files.writeString(ttlFile, "@prefix ex: <http://example.com/> .\nex:s ex:p \"o\" .\n", StandardCharsets.UTF_8);

		Class<?> helperClass = Class.forName("com.the_qa_company.qendpoint.benchmarks.BenchmarkDatasetFiles");
		Method ensureMethod = helperClass.getDeclaredMethod("ensureDatagovbeValidNtGz", Path.class);

		Object result = ensureMethod.invoke(null, repoRoot);
		assertNotNull(result);
		assertTrue(result instanceof Path);

		Path ntGzFile = (Path) result;
		assertTrue(Files.exists(ntGzFile));

		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new GZIPInputStream(Files.newInputStream(ntGzFile)), StandardCharsets.UTF_8))) {
			String line = reader.readLine();
			assertNotNull(line);
			assertTrue(line.contains("<http://example.com/s>"));
			assertTrue(line.contains("<http://example.com/p>"));
		}
	}
}
