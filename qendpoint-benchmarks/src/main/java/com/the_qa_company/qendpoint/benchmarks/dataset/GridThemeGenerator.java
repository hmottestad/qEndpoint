package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.GRID_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_INT;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class GridThemeGenerator {
	private GridThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int substationCount = jitterInt(jitterRandom, scaled(120, config.scale()), 2);
		int transformersPerSubstation = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int linesPerSubstation = jitterInt(jitterRandom, scaled(1, config.scale()), 1);
		int metersPerTransformer = jitterInt(jitterRandom, scaled(2, config.scale()), 1);

		String substationType = iri(GRID_NS, "Substation");
		String transformerType = iri(GRID_NS, "Transformer");
		String lineType = iri(GRID_NS, "Line");
		String meterType = iri(GRID_NS, "Meter");
		String loadType = iri(GRID_NS, "Load");
		String generatorType = iri(GRID_NS, "Generator");

		String feeds = iri(GRID_NS, "feeds");
		String connectsTo = iri(GRID_NS, "connectsTo");
		String hasMeter = iri(GRID_NS, "hasMeter");
		String measures = iri(GRID_NS, "measures");
		String loadValue = iri(GRID_NS, "loadValue");
		String capacity = iri(GRID_NS, "capacity");
		String hasName = iri(GRID_NS, "name");

		List<String> substations = new ArrayList<>(substationCount);
		for (int s = 0; s < substationCount; s++) {
			String substation = entity(GRID_NS, "substation", s);
			substations.add(substation);
			writer.iriStatement(substation, RDF_TYPE, substationType);
			writer.literalStatement(substation, hasName, "Substation " + s);
		}

		int transformerIndex = 0;
		int lineIndex = 0;
		int meterIndex = 0;
		int loadIndex = 0;
		for (int s = 0; s < substations.size(); s++) {
			String substation = substations.get(s);
			for (int t = 0; t < transformersPerSubstation; t++) {
				String transformer = entity(GRID_NS, "transformer", transformerIndex++);
				writer.iriStatement(transformer, RDF_TYPE, transformerType);
				writer.iriStatement(transformer, feeds, substation);
				for (int m = 0; m < metersPerTransformer; m++) {
					String meter = entity(GRID_NS, "meter", meterIndex++);
					String load = entity(GRID_NS, "load", loadIndex++);
					writer.iriStatement(meter, RDF_TYPE, meterType);
					writer.iriStatement(load, RDF_TYPE, loadType);
					writer.iriStatement(meter, measures, load);
					writer.iriStatement(transformer, hasMeter, meter);
					writer.literalStatement(load, loadValue, Integer.toString(50 + contentRandom.nextInt(150)),
							XSD_INT);
				}
			}

			for (int l = 0; l < linesPerSubstation; l++) {
				String line = entity(GRID_NS, "line", lineIndex++);
				String target = substations.get(contentRandom.nextInt(substations.size()));
				writer.iriStatement(line, RDF_TYPE, lineType);
				writer.iriStatement(line, connectsTo, substation);
				writer.iriStatement(line, connectsTo, target);
			}

			String generator = entity(GRID_NS, "generator", s);
			writer.iriStatement(generator, RDF_TYPE, generatorType);
			writer.iriStatement(generator, feeds, substation);
			writer.literalStatement(generator, capacity, Integer.toString(500 + contentRandom.nextInt(500)), XSD_INT);
		}
	}
}
