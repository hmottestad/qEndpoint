package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.CONNECTED_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_INT;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterDouble;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class ConnectedThemeGenerator {
	private ConnectedThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int nodeCount = jitterInt(jitterRandom, scaled(500, config.scale()), 2);
		int hubCount = Math.min(nodeCount, jitterInt(jitterRandom, scaled(10, config.scale()), 1));
		int edgesPerNode = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		double hubBias = jitterDouble(jitterRandom, 0.6, 0.0, 1.0);

		String nodeType = iri(CONNECTED_NS, "Node");
		String connectsTo = iri(CONNECTED_NS, "connectsTo");
		String linkWeight = iri(CONNECTED_NS, "weight");

		List<String> nodes = new ArrayList<>(nodeCount);
		for (int i = 0; i < nodeCount; i++) {
			String node = entity(CONNECTED_NS, "node", i);
			nodes.add(node);
			writer.iriStatement(node, RDF_TYPE, nodeType);
		}

		List<String> hubs = nodes.subList(0, hubCount);
		for (String node : nodes) {
			for (int e = 0; e < edgesPerNode; e++) {
				String target = contentRandom.nextDouble() < hubBias ? hubs.get(contentRandom.nextInt(hubs.size()))
						: nodes.get(contentRandom.nextInt(nodes.size()));
				if (!node.equals(target)) {
					writer.iriStatement(node, connectsTo, target);
					writer.literalStatement(node, linkWeight, Integer.toString(1 + contentRandom.nextInt(10)), XSD_INT);
				}
			}
		}
	}
}
