package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentInputStreamPipeSizeTest {
	private static final int JENA_BUFFER_BYTES = 131072;
	private static final int MAX_PIPE_BYTES = 1024 * 1024;

	@Test
	public void concurrentInputStreamPipeSizeIsBounded() throws Exception {
		assertPipeSize(readPipeSizeBytes(ConcurrentInputStream.class));
	}

	@Test
	public void chunkedConcurrentInputStreamPipeSizeIsBounded() throws Exception {
		assertPipeSize(readPipeSizeBytes(ChunkedConcurrentInputStream.class));
	}

	private static int readPipeSizeBytes(Class<?> type) throws Exception {
		Field field;
		try {
			field = type.getDeclaredField("PIPE_SIZE_BYTES");
		} catch (NoSuchFieldException e) {
			fail("Missing PIPE_SIZE_BYTES in " + type.getName());
			return -1;
		}
		field.setAccessible(true);
		return (int) field.get(null);
	}

	private static void assertPipeSize(int size) {
		assertTrue("Pipe size should exceed Jena buffer (131072 bytes)", size > JENA_BUFFER_BYTES);
		assertTrue("Pipe size should stay bounded to avoid OOM", size <= MAX_PIPE_BYTES);
	}
}
