package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.util.Random;

final class ThemeDatasetSupport {
	static final String BASE = "http://example.com/theme/";
	static final String MEDICAL_NS = BASE + "medical/";
	static final String SOCIAL_NS = BASE + "social/";
	static final String LIBRARY_NS = BASE + "library/";
	static final String ENGINEERING_NS = BASE + "engineering/";
	static final String CONNECTED_NS = BASE + "connected/";
	static final String TRAIN_NS = BASE + "train/";
	static final String GRID_NS = BASE + "grid/";
	static final String PHARMA_NS = BASE + "pharma/";

	static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	static final String XSD_INT = "http://www.w3.org/2001/XMLSchema#integer";
	static final String XSD_DATE = "http://www.w3.org/2001/XMLSchema#date";
	static final String XSD_DATE_TIME = "http://www.w3.org/2001/XMLSchema#dateTime";
	static final String XSD_TIME = "http://www.w3.org/2001/XMLSchema#time";
	static final String XSD_DOUBLE = "http://www.w3.org/2001/XMLSchema#double";

	static final long JITTER_SEED_XOR = 0x9E3779B97F4A7C15L;

	static final String[] WORDS = new String[] { "alpha", "beta", "gamma", "delta", "epsilon", "omega", "vector",
			"node", "graph", "system", "signal", "sensor", "dataset", "record", "event", "network", "route", "station",
			"engine", "grid", "current", "voltage", "load", "train", "cable", "user", "profile", "post", "comment" };

	private ThemeDatasetSupport() {
	}

	static int scaled(int base, int scale) {
		return Math.max(1, base * Math.max(1, scale));
	}

	static Random jitterRandom(long seed) {
		return new Random(seed ^ JITTER_SEED_XOR);
	}

	static int jitterInt(Random random, int base, int minValue) {
		int delta = base / 2;
		int min = Math.max(minValue, base - delta);
		int max = Math.max(min, base + delta);
		if (min == max) {
			return min;
		}
		return min + random.nextInt(max - min + 1);
	}

	static double jitterDouble(Random random, double base, double minValue, double maxValue) {
		double delta = base * 0.5;
		double min = Math.max(minValue, base - delta);
		double max = Math.min(maxValue, base + delta);
		if (max < min) {
			double tmp = min;
			min = max;
			max = tmp;
		}
		if (max == min) {
			return min;
		}
		return min + random.nextDouble() * (max - min);
	}

	static String iri(String namespace, String localName) {
		return namespace + localName;
	}

	static String entity(String namespace, String category, int id) {
		return namespace + category + "/" + id;
	}

	static String randomWord(Random random) {
		return WORDS[random.nextInt(WORDS.length)];
	}

	static String randomSentence(Random random, int minWords, int maxWords) {
		int total = minWords + random.nextInt(Math.max(1, maxWords - minWords + 1));
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < total; i++) {
			if (i > 0) {
				builder.append(' ');
			}
			builder.append(randomWord(random));
		}
		return builder.toString();
	}

	static int requirePositive(int value, String name) {
		if (value <= 0) {
			throw new IllegalArgumentException(name + " must be > 0");
		}
		return value;
	}
}
