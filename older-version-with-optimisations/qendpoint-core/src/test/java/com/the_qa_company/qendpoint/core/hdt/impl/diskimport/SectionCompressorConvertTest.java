package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SectionCompressorConvertTest {

	@Test
	public void convertDoesNotCopyImmutableByteStrings() {
		SectionCompressor compressor = new SectionCompressor(null, null, 0, 0L, 0, false, false, CompressionType.NONE);

		CompactString subject = new CompactString("http://ex/s");
		CompactString predicate = new CompactString("http://ex/p");
		CompactString object = new CompactString("\"o\"");
		CompactString graph = new CompactString("http://ex/g");

		assertSame(subject, compressor.convertSubject(subject));
		assertSame(predicate, compressor.convertPredicate(predicate));
		assertSame(object, compressor.convertObject(object));
		assertSame(graph, compressor.convertGraph(graph));
	}

	@Test
	public void convertCopiesMutableByteStrings() {
		SectionCompressor compressor = new SectionCompressor(null, null, 0, 0L, 0, false, false, CompressionType.NONE);

		ReplazableString mutable = new ReplazableString();
		mutable.appendNoCompact("http://ex/s");

		ByteString converted = compressor.convertSubject(mutable);
		assertTrue(converted instanceof CompactString);
		assertNotSame(mutable, converted);
		assertEquals(mutable.toString(), converted.toString());
	}
}
