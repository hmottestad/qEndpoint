package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

final class NTriplesWriter implements Closeable {
	private final BufferedWriter writer;

	NTriplesWriter(OutputStream output) {
		this.writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
	}

	void iriStatement(String subjectIri, String predicateIri, String objectIri) throws IOException {
		writeLine(formatIri(subjectIri), formatIri(predicateIri), formatIri(objectIri));
	}

	void literalStatement(String subjectIri, String predicateIri, String literal) throws IOException {
		writeLine(formatIri(subjectIri), formatIri(predicateIri), formatLiteral(literal));
	}

	void literalStatement(String subjectIri, String predicateIri, String literal, String datatypeIri)
			throws IOException {
		writeLine(formatIri(subjectIri), formatIri(predicateIri),
				formatLiteral(literal) + "^^" + formatIri(datatypeIri));
	}

	private void writeLine(String subject, String predicate, String object) throws IOException {
		writer.write(subject);
		writer.write(' ');
		writer.write(predicate);
		writer.write(' ');
		writer.write(object);
		writer.write(" .\n");
	}

	private static String formatIri(String iri) {
		return "<" + iri + ">";
	}

	private static String formatLiteral(String literal) {
		return "\"" + escapeLiteral(literal) + "\"";
	}

	private static String escapeLiteral(String literal) {
		StringBuilder sb = new StringBuilder(literal.length());
		for (int i = 0; i < literal.length(); i++) {
			char c = literal.charAt(i);
			switch (c) {
			case '\\':
				sb.append("\\\\");
				break;
			case '"':
				sb.append("\\\"");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			default:
				sb.append(c);
				break;
			}
		}
		return sb.toString();
	}

	@Override
	public void close() throws IOException {
		writer.flush();
		writer.close();
	}
}
