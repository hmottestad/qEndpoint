package com.the_qa_company.qendpoint.benchmarks;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

public class ThemeDatasetGeneratorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void generateMedicalRecordsWritesNTriples() throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		Class<?> generatorClass = Class
				.forName("com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetGenerator");
		Class<?> configClass = Class
				.forName("com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetGenerator$GenerationConfig");
		Class<?> themeClass = Class
				.forName("com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetGenerator$Theme");

		Method defaultsMethod = configClass.getDeclaredMethod("defaults");
		Object config = defaultsMethod.invoke(null);
		Method withScaleMethod = configClass.getDeclaredMethod("withScale", int.class);
		config = withScaleMethod.invoke(config, 1);
		Method withSeedMethod = configClass.getDeclaredMethod("withSeed", long.class);
		config = withSeedMethod.invoke(config, 7L);

		Object theme = Enum.valueOf((Class<Enum>) themeClass, "MEDICAL_RECORDS");
		Method generateMethod = generatorClass.getDeclaredMethod("generate", themeClass, configClass,
				OutputStream.class);
		generateMethod.invoke(null, theme, config, out);

		String nt = out.toString(StandardCharsets.UTF_8);
		assertTrue(nt.contains("<http://example.com/theme/medical/patient/0>"));
		assertTrue(nt.contains("<http://example.com/theme/medical/hasEncounter>"));
		assertTrue(nt.contains(" .\n"));
	}
}
