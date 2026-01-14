package com.the_qa_company.qendpoint.core.compact.sequence;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

public class SequenceLog64BigDiskBulkSetTest {
	@Test
	public void bulkSetWritesExpectedValues() throws Exception {
		Path file = Files.createTempFile("SequenceLog64BigDiskBulkSetTest", ".bin");
		Files.deleteIfExists(file);

		try (SequenceLog64BigDisk sequence = new SequenceLog64BigDisk(file, 7, 32, true)) {
			long[] positions = { 1, 9, 10, 15, 16, 17 };
			long[] values = { 11, 22, 23, 44, 45, 46 };

			Method bulkSet = SequenceLog64BigDisk.class.getMethod("set", long[].class, long[].class, int.class,
					int.class);
			bulkSet.invoke(sequence, positions, values, 0, positions.length);

			for (int i = 0; i < positions.length; i++) {
				Assert.assertEquals(values[i], sequence.get(positions[i]));
			}
		}
	}
}
