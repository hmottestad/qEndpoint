package com.the_qa_company.qendpoint.benchmarks;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ThemeDatasetFilesTest {

	@Test
	public void ensureThemeMixedNtGz_createsFileWithThemes() throws Exception {
		String oldScale = System.getProperty("qendpoint.benchmarks.theme.scale");
		String oldSeed = System.getProperty("qendpoint.benchmarks.theme.seed");
		System.setProperty("qendpoint.benchmarks.theme.scale", "1");
		System.setProperty("qendpoint.benchmarks.theme.seed", "11");

		Path repoRoot = Files.createTempDirectory("qendpoint-benchmarks");
		Path indexingDir = repoRoot.resolve("indexing");
		Files.createDirectories(indexingDir);

		try {
			Class<?> helperClass = Class.forName("com.the_qa_company.qendpoint.benchmarks.BenchmarkDatasetFiles");
			Method ensureMethod = helperClass.getDeclaredMethod("ensureThemeMixedNtGz", Path.class);

			Object result = ensureMethod.invoke(null, repoRoot);
			assertNotNull(result);
			assertTrue(result instanceof Path);

			Path ntGzFile = (Path) result;
			assertTrue(Files.exists(ntGzFile));

			try (BufferedReader reader = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(Files.newInputStream(ntGzFile)), StandardCharsets.UTF_8))) {
				boolean sawMedical = false;
				boolean sawSocial = false;
				boolean sawLibrary = false;
				String line;
				while ((line = reader.readLine()) != null) {
					sawMedical |= line.contains("theme/medical/");
					sawSocial |= line.contains("theme/social/");
					sawLibrary |= line.contains("theme/library/");
					if (sawMedical && (sawSocial || sawLibrary)) {
						break;
					}
				}
				assertTrue(sawMedical);
				assertTrue(sawSocial || sawLibrary);
			}
		} finally {
			if (oldScale == null) {
				System.clearProperty("qendpoint.benchmarks.theme.scale");
			} else {
				System.setProperty("qendpoint.benchmarks.theme.scale", oldScale);
			}
			if (oldSeed == null) {
				System.clearProperty("qendpoint.benchmarks.theme.seed");
			} else {
				System.setProperty("qendpoint.benchmarks.theme.seed", oldSeed);
			}
		}
	}

	@Test
	public void resolveOrPreparePath_generatesThemeMixedDataset() throws Exception {
		String oldScale = System.getProperty("qendpoint.benchmarks.theme.scale");
		System.setProperty("qendpoint.benchmarks.theme.scale", "1");

		try {
			Class<?> stateClass = Class
					.forName("com.the_qa_company.qendpoint.benchmarks.NtGzToHdtAndIndexesBenchmark$BenchmarkState");
			Method resolveMethod = stateClass.getDeclaredMethod("resolveOrPreparePath", String.class);
			resolveMethod.setAccessible(true);

			Path result = (Path) resolveMethod.invoke(null, "indexing/theme-mixed.nt.gz");
			assertNotNull(result);
			assertTrue(result.isAbsolute());
			assertTrue(result.toString().endsWith("indexing/theme-mixed.nt.gz"));
			assertTrue(Files.exists(result));
		} finally {
			if (oldScale == null) {
				System.clearProperty("qendpoint.benchmarks.theme.scale");
			} else {
				System.setProperty("qendpoint.benchmarks.theme.scale", oldScale);
			}
		}
	}
}
