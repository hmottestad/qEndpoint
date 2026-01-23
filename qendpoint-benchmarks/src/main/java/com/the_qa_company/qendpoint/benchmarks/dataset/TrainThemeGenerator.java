package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.TRAIN_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_TIME;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class TrainThemeGenerator {
	private TrainThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int stationCount = jitterInt(jitterRandom, scaled(200, config.scale()), 2);
		int routeCount = jitterInt(jitterRandom, scaled(30, config.scale()), 2);
		int stopsPerRoute = jitterInt(jitterRandom, scaled(5, config.scale()), 2);
		int trainCount = jitterInt(jitterRandom, scaled(50, config.scale()), 2);
		int tripsPerTrain = jitterInt(jitterRandom, scaled(2, config.scale()), 1);

		String operationalPointType = iri(TRAIN_NS, "OperationalPoint");
		String lineType = iri(TRAIN_NS, "Line");
		String sectionType = iri(TRAIN_NS, "SectionOfLine");
		String trackSectionType = iri(TRAIN_NS, "TrackSection");
		String serviceType = iri(TRAIN_NS, "TrainService");

		String partOfLine = iri(TRAIN_NS, "partOfLine");
		String connectsOperationalPoint = iri(TRAIN_NS, "connectsOperationalPoint");
		String hasTrackSection = iri(TRAIN_NS, "hasTrackSection");
		String trackSectionOf = iri(TRAIN_NS, "trackSectionOf");
		String runsOnSection = iri(TRAIN_NS, "runsOnSection");
		String passesThrough = iri(TRAIN_NS, "passesThrough");
		String scheduledTime = iri(TRAIN_NS, "scheduledTime");
		String hasName = iri(TRAIN_NS, "name");

		List<String> operationalPoints = new ArrayList<>(stationCount);
		for (int s = 0; s < stationCount; s++) {
			String point = entity(TRAIN_NS, "operational-point", s);
			operationalPoints.add(point);
			writer.iriStatement(point, RDF_TYPE, operationalPointType);
			writer.literalStatement(point, hasName, "OP " + s);
		}

		List<String> lines = new ArrayList<>(routeCount);
		for (int l = 0; l < routeCount; l++) {
			String line = entity(TRAIN_NS, "line", l);
			lines.add(line);
			writer.iriStatement(line, RDF_TYPE, lineType);
			writer.literalStatement(line, hasName, "Line " + l);
		}

		List<String> sections = new ArrayList<>();
		int sectionIndex = 0;
		int trackIndex = 0;
		int stationOffset = 0;
		for (String line : lines) {
			for (int s = 0; s < stopsPerRoute; s++) {
				String section = entity(TRAIN_NS, "section", sectionIndex++);
				sections.add(section);
				writer.iriStatement(section, RDF_TYPE, sectionType);
				writer.iriStatement(section, partOfLine, line);
				String start = operationalPoints.get((stationOffset + s) % operationalPoints.size());
				String end = operationalPoints.get((stationOffset + s + 1) % operationalPoints.size());
				writer.iriStatement(section, connectsOperationalPoint, start);
				writer.iriStatement(section, connectsOperationalPoint, end);

				String trackSection = entity(TRAIN_NS, "track-section", trackIndex++);
				writer.iriStatement(trackSection, RDF_TYPE, trackSectionType);
				writer.iriStatement(section, hasTrackSection, trackSection);
				writer.iriStatement(trackSection, trackSectionOf, section);
			}
			stationOffset += stopsPerRoute;
		}

		int serviceIndex = 0;
		for (int t = 0; t < trainCount; t++) {
			String service = entity(TRAIN_NS, "service", serviceIndex++);
			writer.iriStatement(service, RDF_TYPE, serviceType);
			writer.literalStatement(service, hasName, "Service " + t);
			for (int tr = 0; tr < tripsPerTrain; tr++) {
				String section = sections.get(contentRandom.nextInt(sections.size()));
				String operationalPoint = operationalPoints.get(contentRandom.nextInt(operationalPoints.size()));
				writer.iriStatement(service, runsOnSection, section);
				writer.iriStatement(service, passesThrough, operationalPoint);
				writer.literalStatement(service, scheduledTime,
						LocalTime.of(5, 0).plusMinutes(contentRandom.nextInt(600)).toString(), XSD_TIME);
			}
		}
	}
}
