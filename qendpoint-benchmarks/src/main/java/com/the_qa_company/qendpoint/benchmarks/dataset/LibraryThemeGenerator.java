package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.LIBRARY_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_DATE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.randomWord;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class LibraryThemeGenerator {
	private LibraryThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int authorCount = jitterInt(jitterRandom, scaled(100, config.scale()), 2);
		int branchCount = jitterInt(jitterRandom, scaled(5, config.scale()), 1);
		int bookCount = jitterInt(jitterRandom, scaled(400, config.scale()), 2);
		int memberCount = jitterInt(jitterRandom, scaled(120, config.scale()), 2);
		int copiesPerBook = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int authorsPerBook = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int loansPerMember = jitterInt(jitterRandom, scaled(1, config.scale()), 1);

		String bookType = iri(LIBRARY_NS, "Book");
		String authorType = iri(LIBRARY_NS, "Author");
		String copyType = iri(LIBRARY_NS, "Copy");
		String memberType = iri(LIBRARY_NS, "Member");
		String loanType = iri(LIBRARY_NS, "Loan");
		String branchType = iri(LIBRARY_NS, "Branch");

		String writtenBy = iri(LIBRARY_NS, "writtenBy");
		String hasCopy = iri(LIBRARY_NS, "hasCopy");
		String locatedAt = iri(LIBRARY_NS, "locatedAt");
		String borrowedBy = iri(LIBRARY_NS, "borrowedBy");
		String loanedCopy = iri(LIBRARY_NS, "loanedCopy");
		String loanDate = iri(LIBRARY_NS, "loanDate");
		String dueDate = iri(LIBRARY_NS, "dueDate");
		String hasName = iri(LIBRARY_NS, "name");
		String title = iri(LIBRARY_NS, "title");

		List<String> authors = new ArrayList<>(authorCount);
		for (int a = 0; a < authorCount; a++) {
			String author = entity(LIBRARY_NS, "author", a);
			authors.add(author);
			writer.iriStatement(author, RDF_TYPE, authorType);
			writer.literalStatement(author, hasName, "Author " + a);
		}

		List<String> branches = new ArrayList<>(branchCount);
		for (int b = 0; b < branchCount; b++) {
			String branch = entity(LIBRARY_NS, "branch", b);
			branches.add(branch);
			writer.iriStatement(branch, RDF_TYPE, branchType);
			writer.literalStatement(branch, hasName, "Branch " + b);
		}

		List<String> copies = new ArrayList<>();
		int copyIndex = 0;
		for (int b = 0; b < bookCount; b++) {
			String book = entity(LIBRARY_NS, "book", b);
			writer.iriStatement(book, RDF_TYPE, bookType);
			writer.literalStatement(book, title, "Book " + b + " " + randomWord(contentRandom));

			for (int a = 0; a < authorsPerBook; a++) {
				String author = authors.get(contentRandom.nextInt(authors.size()));
				writer.iriStatement(book, writtenBy, author);
			}

			for (int c = 0; c < copiesPerBook; c++) {
				String copy = entity(LIBRARY_NS, "copy", copyIndex++);
				copies.add(copy);
				writer.iriStatement(copy, RDF_TYPE, copyType);
				writer.iriStatement(copy, locatedAt, branches.get(contentRandom.nextInt(branches.size())));
				writer.iriStatement(book, hasCopy, copy);
			}
		}

		List<String> members = new ArrayList<>(memberCount);
		for (int m = 0; m < memberCount; m++) {
			String member = entity(LIBRARY_NS, "member", m);
			members.add(member);
			writer.iriStatement(member, RDF_TYPE, memberType);
			writer.literalStatement(member, hasName, "Member " + m);
		}

		int loanIndex = 0;
		for (String member : members) {
			for (int l = 0; l < loansPerMember; l++) {
				String loan = entity(LIBRARY_NS, "loan", loanIndex++);
				String copy = copies.get(contentRandom.nextInt(copies.size()));
				LocalDate date = LocalDate.of(2024, 1, 1).plusDays(contentRandom.nextInt(90));
				writer.iriStatement(loan, RDF_TYPE, loanType);
				writer.iriStatement(loan, borrowedBy, member);
				writer.iriStatement(loan, loanedCopy, copy);
				writer.literalStatement(loan, loanDate, date.toString(), XSD_DATE);
				writer.literalStatement(loan, dueDate, date.plusDays(14).toString(), XSD_DATE);
			}
		}
	}
}
