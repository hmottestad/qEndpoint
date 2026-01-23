package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.ENGINEERING_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_DOUBLE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class EngineeringThemeGenerator {
	private EngineeringThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int assemblyCount = jitterInt(jitterRandom, scaled(50, config.scale()), 1);
		int componentCount = jitterInt(jitterRandom, scaled(600, config.scale()), 2);
		int requirementCount = jitterInt(jitterRandom, scaled(80, config.scale()), 2);
		int testsPerRequirement = jitterInt(jitterRandom, scaled(1, config.scale()), 1);

		String assemblyType = iri(ENGINEERING_NS, "Assembly");
		String componentType = iri(ENGINEERING_NS, "Component");
		String requirementType = iri(ENGINEERING_NS, "Requirement");
		String testType = iri(ENGINEERING_NS, "TestCase");
		String measurementType = iri(ENGINEERING_NS, "Measurement");

		String partOf = iri(ENGINEERING_NS, "partOf");
		String dependsOn = iri(ENGINEERING_NS, "dependsOn");
		String satisfies = iri(ENGINEERING_NS, "satisfies");
		String verifiedBy = iri(ENGINEERING_NS, "verifiedBy");
		String measuredValue = iri(ENGINEERING_NS, "measuredValue");
		String hasName = iri(ENGINEERING_NS, "name");

		List<String> assemblies = new ArrayList<>(assemblyCount);
		for (int a = 0; a < assemblyCount; a++) {
			String assembly = entity(ENGINEERING_NS, "assembly", a);
			assemblies.add(assembly);
			writer.iriStatement(assembly, RDF_TYPE, assemblyType);
			writer.literalStatement(assembly, hasName, "Assembly " + a);
		}

		List<String> components = new ArrayList<>(componentCount);
		for (int c = 0; c < componentCount; c++) {
			String component = entity(ENGINEERING_NS, "component", c);
			components.add(component);
			writer.iriStatement(component, RDF_TYPE, componentType);
			writer.literalStatement(component, hasName, "Component " + c);
			writer.iriStatement(component, partOf, assemblies.get(contentRandom.nextInt(assemblies.size())));
			if (components.size() > 1) {
				String dependency = components.get(contentRandom.nextInt(components.size() - 1));
				writer.iriStatement(component, dependsOn, dependency);
			}
		}

		int testIndex = 0;
		int measurementIndex = 0;
		for (int r = 0; r < requirementCount; r++) {
			String requirement = entity(ENGINEERING_NS, "requirement", r);
			writer.iriStatement(requirement, RDF_TYPE, requirementType);
			writer.literalStatement(requirement, hasName, "REQ-" + (1000 + r));
			String component = components.get(contentRandom.nextInt(components.size()));
			writer.iriStatement(requirement, satisfies, component);
			for (int t = 0; t < testsPerRequirement; t++) {
				String test = entity(ENGINEERING_NS, "test", testIndex++);
				writer.iriStatement(test, RDF_TYPE, testType);
				writer.iriStatement(requirement, verifiedBy, test);
				String measurement = entity(ENGINEERING_NS, "measurement", measurementIndex++);
				writer.iriStatement(measurement, RDF_TYPE, measurementType);
				writer.literalStatement(measurement, measuredValue,
						Double.toString(0.8 + contentRandom.nextDouble() * 0.2), XSD_DOUBLE);
				writer.iriStatement(test, verifiedBy, measurement);
			}
		}
	}
}
