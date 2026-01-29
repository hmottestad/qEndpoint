package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public final class ThemeDatasetGenerator {

	public enum Theme {
		MEDICAL_RECORDS, SOCIAL_MEDIA, LIBRARY, ENGINEERING, HIGHLY_CONNECTED, TRAIN, ELECTRICAL_GRID, PHARMA, MIXED
	}

	public static final class GenerationConfig {
		private int scale = 1;
		private long seed = 42L;

		public static GenerationConfig defaults() {
			return new GenerationConfig();
		}

		public GenerationConfig withScale(int scale) {
			this.scale = ThemeDatasetSupport.requirePositive(scale, "scale");
			return this;
		}

		public GenerationConfig withSeed(long seed) {
			this.seed = seed;
			return this;
		}

		public int scale() {
			return scale;
		}

		public long seed() {
			return seed;
		}

		public static GenerationConfig fromSystemProperties() {
			GenerationConfig config = defaults();
			String scaleValue = System.getProperty("qendpoint.benchmarks.theme.scale");
			if (scaleValue != null && !scaleValue.isBlank()) {
				config.withScale(Integer.parseInt(scaleValue));
			}
			String seedValue = System.getProperty("qendpoint.benchmarks.theme.seed");
			if (seedValue != null && !seedValue.isBlank()) {
				config.withSeed(Long.parseLong(seedValue));
			}
			return config;
		}
	}

	private ThemeDatasetGenerator() {
	}

	public static void generate(Theme theme, GenerationConfig config, OutputStream output) throws IOException {
		Objects.requireNonNull(theme, "theme");
		Objects.requireNonNull(config, "config");
		Objects.requireNonNull(output, "output");
		try (NTriplesWriter writer = new NTriplesWriter(output)) {
			generate(theme, config, writer);
		}
	}

	static void generate(Theme theme, GenerationConfig config, NTriplesWriter writer) throws IOException {
		switch (theme) {
		case MEDICAL_RECORDS:
			MedicalThemeGenerator.generate(config, writer);
			break;
		case SOCIAL_MEDIA:
			SocialThemeGenerator.generate(config, writer);
			break;
		case LIBRARY:
			LibraryThemeGenerator.generate(config, writer);
			break;
		case ENGINEERING:
			EngineeringThemeGenerator.generate(config, writer);
			break;
		case HIGHLY_CONNECTED:
			ConnectedThemeGenerator.generate(config, writer);
			break;
		case TRAIN:
			TrainThemeGenerator.generate(config, writer);
			break;
		case ELECTRICAL_GRID:
			GridThemeGenerator.generate(config, writer);
			break;
		case PHARMA:
			PharmaThemeGenerator.generate(config, writer);
			break;
		case MIXED:
			MedicalThemeGenerator.generate(config, writer);
			SocialThemeGenerator.generate(config, writer);
			LibraryThemeGenerator.generate(config, writer);
			EngineeringThemeGenerator.generate(config, writer);
			ConnectedThemeGenerator.generate(config, writer);
			TrainThemeGenerator.generate(config, writer);
			GridThemeGenerator.generate(config, writer);
			PharmaThemeGenerator.generate(config, writer);
			break;
		default:
			throw new IllegalArgumentException("Unsupported theme " + theme);
		}
	}
}
