package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.MEDICAL_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_DATE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_INT;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.randomWord;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class MedicalThemeGenerator {
	private MedicalThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int patientCount = jitterInt(jitterRandom, scaled(200, config.scale()), 1);
		int practitionerCount = jitterInt(jitterRandom, scaled(60, config.scale()), 1);
		int encountersPerPatient = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int conditionsPerEncounter = jitterInt(jitterRandom, scaled(1, config.scale()), 1);
		int observationsPerEncounter = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int medicationsPerPatient = jitterInt(jitterRandom, scaled(1, config.scale()), 1);

		String patientType = iri(MEDICAL_NS, "Patient");
		String practitionerType = iri(MEDICAL_NS, "Practitioner");
		String encounterType = iri(MEDICAL_NS, "Encounter");
		String conditionType = iri(MEDICAL_NS, "Condition");
		String observationType = iri(MEDICAL_NS, "Observation");
		String medicationType = iri(MEDICAL_NS, "Medication");

		String hasName = iri(MEDICAL_NS, "name");
		String hasEncounter = iri(MEDICAL_NS, "hasEncounter");
		String handledBy = iri(MEDICAL_NS, "handledBy");
		String recordedOn = iri(MEDICAL_NS, "recordedOn");
		String hasCondition = iri(MEDICAL_NS, "hasCondition");
		String hasObservation = iri(MEDICAL_NS, "hasObservation");
		String observationValue = iri(MEDICAL_NS, "value");
		String hasMedication = iri(MEDICAL_NS, "hasMedication");
		String hasCode = iri(MEDICAL_NS, "code");

		List<String> practitioners = new ArrayList<>(practitionerCount);
		for (int i = 0; i < practitionerCount; i++) {
			String practitioner = entity(MEDICAL_NS, "practitioner", i);
			practitioners.add(practitioner);
			writer.iriStatement(practitioner, RDF_TYPE, practitionerType);
			writer.literalStatement(practitioner, hasName, "Dr " + randomWord(contentRandom) + " " + i);
		}

		int encounterIndex = 0;
		int conditionIndex = 0;
		int observationIndex = 0;
		int medicationIndex = 0;
		for (int p = 0; p < patientCount; p++) {
			String patient = entity(MEDICAL_NS, "patient", p);
			writer.iriStatement(patient, RDF_TYPE, patientType);
			writer.literalStatement(patient, hasName, "Patient " + p);

			for (int m = 0; m < medicationsPerPatient; m++) {
				String medication = entity(MEDICAL_NS, "medication", medicationIndex++);
				writer.iriStatement(medication, RDF_TYPE, medicationType);
				writer.literalStatement(medication, hasCode, "MED-" + (1000 + m));
				writer.iriStatement(patient, hasMedication, medication);
			}

			for (int e = 0; e < encountersPerPatient; e++) {
				String encounter = entity(MEDICAL_NS, "encounter", encounterIndex++);
				writer.iriStatement(encounter, RDF_TYPE, encounterType);
				writer.iriStatement(patient, hasEncounter, encounter);
				writer.literalStatement(encounter, recordedOn,
						LocalDate.of(2024, 1, 1).plusDays(contentRandom.nextInt(365)).toString(), XSD_DATE);
				String practitioner = practitioners.get(contentRandom.nextInt(practitioners.size()));
				writer.iriStatement(encounter, handledBy, practitioner);

				for (int c = 0; c < conditionsPerEncounter; c++) {
					String condition = entity(MEDICAL_NS, "condition", conditionIndex++);
					writer.iriStatement(condition, RDF_TYPE, conditionType);
					writer.literalStatement(condition, hasCode, "DX-" + (200 + c));
					writer.iriStatement(encounter, hasCondition, condition);
				}

				for (int o = 0; o < observationsPerEncounter; o++) {
					String observation = entity(MEDICAL_NS, "observation", observationIndex++);
					writer.iriStatement(observation, RDF_TYPE, observationType);
					writer.literalStatement(observation, observationValue,
							Integer.toString(50 + contentRandom.nextInt(50)), XSD_INT);
					writer.iriStatement(encounter, hasObservation, observation);
				}
			}
		}
	}
}
