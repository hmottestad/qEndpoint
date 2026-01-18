package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.hdt.impl.HDTDiskImporter;
import org.junit.Test;

import java.nio.file.Path;

import static org.junit.Assert.fail;

public class HDTDiskImporterPathTest {
	@Test
	public void exposesPathBasedNTriplesImport() {
		try {
			HDTDiskImporter.class.getMethod("runAllStepsNTriples", Path.class, RDFNotation.class);
		} catch (NoSuchMethodException e) {
			fail("Missing Path-based runAllStepsNTriples(Path, RDFNotation) overload");
		}
	}
}
