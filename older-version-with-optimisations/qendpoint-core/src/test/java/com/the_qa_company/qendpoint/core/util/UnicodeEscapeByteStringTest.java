package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnicodeEscapeByteStringTest {

	@Test
	public void unescapeByteStringMatchesUnescapeString() throws Exception {
		assertUnescapeMatches("plain-ascii", 0, "plain-ascii".length());
		assertUnescapeMatches("héllo", 0, "héllo".length());
		assertUnescapeMatches("\"\\t\\n\\r\\\\\\\"\"", 0, "\"\\t\\n\\r\\\\\\\"\"".length());
		assertUnescapeMatches("\"\\u00E9\"", 0, "\"\\u00E9\"".length());
		assertUnescapeMatches("\"\\uD83D\"", 0, "\"\\uD83D\"".length());
		assertUnescapeMatches("\"\\uDE00\"", 0, "\"\\uDE00\"".length());
		assertUnescapeMatches("\"\\uD83D\\uDE00\"", 0, "\"\\uD83D\\uDE00\"".length());
		assertUnescapeMatches("\"\\U0001F600\"", 0, "\"\\U0001F600\"".length());
	}

	private static void assertUnescapeMatches(String s, int start, int end) throws Exception {
		ByteString expected = new CompactString(UnicodeEscape.unescapeString(s, start, end));

		Method m;
		try {
			m = UnicodeEscape.class.getMethod("unescapeByteString", String.class, int.class, int.class);
		} catch (NoSuchMethodException e) {
			fail("Missing method UnicodeEscape.unescapeByteString(String,int,int)");
			return;
		}

		Object out = m.invoke(null, s, start, end);
		assertNotNull(out);
		assertTrue(out instanceof ByteString);
		ByteString actual = (ByteString) out;
		assertEquals(expected.length(), actual.length());
		assertArrayEquals(expected.getBuffer(), actual.getBuffer());
	}
}
