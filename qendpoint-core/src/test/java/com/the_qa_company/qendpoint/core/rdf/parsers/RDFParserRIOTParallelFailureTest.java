package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.fail;

public class RDFParserRIOTParallelFailureTest {

	@Test
	public void parallelParsePropagatesWorkerFailures() throws Exception {
		String payload = "<http://ex/s1> <http://ex/p> \"o1\" .\n" + "<http://ex/s2> <http://ex/p> \"unterminated .\n"
				+ "_:b1 <http://ex/p> \"o3\" .\n";

		InputStream input = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

		try {
			new RDFParserRIOT().doParse(input, "http://base/", RDFNotation.NTRIPLES, true, (triple, pos) -> {}, true);
			fail("Expected parser to surface worker failure");
		} catch (ParserException expected) {
			// expected
		}
	}
}
