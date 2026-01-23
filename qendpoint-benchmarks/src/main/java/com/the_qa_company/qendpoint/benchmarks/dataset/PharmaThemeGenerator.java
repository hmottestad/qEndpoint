package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.PHARMA_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_DOUBLE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.randomWord;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class PharmaThemeGenerator {
	private PharmaThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int chemicalClassCount = jitterInt(jitterRandom, scaled(40, config.scale()), 2);
		int pathwayCount = jitterInt(jitterRandom, scaled(25, config.scale()), 2);
		int targetCount = jitterInt(jitterRandom, scaled(60, config.scale()), 2);
		int moleculeCount = jitterInt(jitterRandom, scaled(120, config.scale()), 2);
		int diseaseCount = jitterInt(jitterRandom, scaled(50, config.scale()), 2);
		int drugCount = jitterInt(jitterRandom, scaled(150, config.scale()), 2);
		int trialCount = jitterInt(jitterRandom, scaled(80, config.scale()), 2);
		int armsPerTrial = jitterInt(jitterRandom, scaled(2, config.scale()), 1);

		String chemicalClassType = iri(PHARMA_NS, "ChemicalClass");
		String pathwayType = iri(PHARMA_NS, "Pathway");
		String targetType = iri(PHARMA_NS, "Target");
		String moleculeType = iri(PHARMA_NS, "Molecule");
		String diseaseType = iri(PHARMA_NS, "Disease");
		String drugType = iri(PHARMA_NS, "Drug");
		String trialType = iri(PHARMA_NS, "ClinicalTrial");
		String armType = iri(PHARMA_NS, "TrialArm");
		String resultType = iri(PHARMA_NS, "TrialResult");

		String hasName = iri(PHARMA_NS, "name");
		String inClass = iri(PHARMA_NS, "inClass");
		String inPathway = iri(PHARMA_NS, "inPathway");
		String targets = iri(PHARMA_NS, "targets");
		String indicatedFor = iri(PHARMA_NS, "indicatedFor");
		String testedIn = iri(PHARMA_NS, "testedIn");
		String studiesDisease = iri(PHARMA_NS, "studiesDisease");
		String hasArm = iri(PHARMA_NS, "hasArm");
		String armDrug = iri(PHARMA_NS, "armDrug");
		String hasResult = iri(PHARMA_NS, "hasResult");
		String endpoint = iri(PHARMA_NS, "endpoint");
		String effectSize = iri(PHARMA_NS, "effectSize");

		List<String> classes = new ArrayList<>(chemicalClassCount);
		for (int c = 0; c < chemicalClassCount; c++) {
			String chemicalClass = entity(PHARMA_NS, "class", c);
			classes.add(chemicalClass);
			writer.iriStatement(chemicalClass, RDF_TYPE, chemicalClassType);
			writer.literalStatement(chemicalClass, hasName, "Class " + c);
		}

		List<String> pathways = new ArrayList<>(pathwayCount);
		for (int p = 0; p < pathwayCount; p++) {
			String pathway = entity(PHARMA_NS, "pathway", p);
			pathways.add(pathway);
			writer.iriStatement(pathway, RDF_TYPE, pathwayType);
			writer.literalStatement(pathway, hasName, "Pathway " + p);
		}

		List<String> targetsList = new ArrayList<>(targetCount);
		for (int t = 0; t < targetCount; t++) {
			String target = entity(PHARMA_NS, "target", t);
			targetsList.add(target);
			writer.iriStatement(target, RDF_TYPE, targetType);
			writer.literalStatement(target, hasName, "Target " + t);
			writer.iriStatement(target, inPathway, pathways.get(contentRandom.nextInt(pathways.size())));
		}

		List<String> molecules = new ArrayList<>(moleculeCount);
		for (int m = 0; m < moleculeCount; m++) {
			String molecule = entity(PHARMA_NS, "molecule", m);
			molecules.add(molecule);
			writer.iriStatement(molecule, RDF_TYPE, moleculeType);
			writer.literalStatement(molecule, hasName, "Molecule " + m);
			writer.iriStatement(molecule, inClass, classes.get(contentRandom.nextInt(classes.size())));
			writer.iriStatement(molecule, targets, targetsList.get(contentRandom.nextInt(targetsList.size())));
		}

		List<String> diseases = new ArrayList<>(diseaseCount);
		for (int d = 0; d < diseaseCount; d++) {
			String disease = entity(PHARMA_NS, "disease", d);
			diseases.add(disease);
			writer.iriStatement(disease, RDF_TYPE, diseaseType);
			writer.literalStatement(disease, hasName, "Disease " + d);
		}

		List<String> drugs = new ArrayList<>(drugCount);
		for (int d = 0; d < drugCount; d++) {
			String drug = entity(PHARMA_NS, "drug", d);
			drugs.add(drug);
			writer.iriStatement(drug, RDF_TYPE, drugType);
			writer.literalStatement(drug, hasName, "Drug " + d);
			writer.iriStatement(drug, targets, targetsList.get(contentRandom.nextInt(targetsList.size())));
			writer.iriStatement(drug, indicatedFor, diseases.get(contentRandom.nextInt(diseases.size())));
			writer.iriStatement(drug, inClass, classes.get(contentRandom.nextInt(classes.size())));
		}

		int armIndex = 0;
		int resultIndex = 0;
		for (int t = 0; t < trialCount; t++) {
			String trial = entity(PHARMA_NS, "trial", t);
			writer.iriStatement(trial, RDF_TYPE, trialType);
			writer.literalStatement(trial, hasName, "Trial " + t);
			writer.iriStatement(trial, studiesDisease, diseases.get(contentRandom.nextInt(diseases.size())));
			for (int a = 0; a < armsPerTrial; a++) {
				String arm = entity(PHARMA_NS, "arm", armIndex++);
				String drug = drugs.get(contentRandom.nextInt(drugs.size()));
				writer.iriStatement(arm, RDF_TYPE, armType);
				writer.iriStatement(trial, hasArm, arm);
				writer.iriStatement(arm, armDrug, drug);
				writer.iriStatement(drug, testedIn, trial);
				String result = entity(PHARMA_NS, "result", resultIndex++);
				writer.iriStatement(result, RDF_TYPE, resultType);
				writer.iriStatement(arm, hasResult, result);
				writer.literalStatement(result, endpoint, randomWord(contentRandom) + "Endpoint");
				writer.literalStatement(result, effectSize, Double.toString(contentRandom.nextDouble()), XSD_DOUBLE);
			}
		}
	}
}
