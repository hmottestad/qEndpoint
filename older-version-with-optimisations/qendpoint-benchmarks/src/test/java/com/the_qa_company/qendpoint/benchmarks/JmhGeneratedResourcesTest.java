package com.the_qa_company.qendpoint.benchmarks;

import static org.junit.Assert.assertNotNull;

import java.io.InputStream;
import org.junit.Test;

public class JmhGeneratedResourcesTest {

	@Test
	public void benchmarkResourcesAreGenerated() throws Exception {
		try (InputStream benchmarkList = getClass().getClassLoader().getResourceAsStream("META-INF/BenchmarkList")) {
			assertNotNull("Missing META-INF/BenchmarkList; JMH annotation processing did not run.", benchmarkList);
		}

		try (InputStream compilerHints = getClass().getClassLoader().getResourceAsStream("META-INF/CompilerHints")) {
			assertNotNull("Missing META-INF/CompilerHints; JMH annotation processing did not run.", compilerHints);
		}
	}
}
