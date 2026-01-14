package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64Big;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.impl.TriplesListLong;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesObjectIndexBucketTest extends AbstractMapMemoryTest {
	@Test
	public void objectIndexWritesAreBucketed() throws Exception {
		HDTSpecification spec = new HDTSpecification();
		TriplesListLong temp = new TriplesListLong(spec);
		temp.insert(1, 1, 2);
		temp.insert(1, 2, 1);
		temp.insert(2, 1, 2);
		temp.insert(2, 2, 1);

		int recordLimit = (int) temp.getNumberOfElements();
		RecordingBitmapTriples triples = new RecordingBitmapTriples(spec, recordLimit);
		triples.load(temp, null);

		Method method = BitmapTriples.class.getDeclaredMethod("createIndexObjectMemoryEfficient");
		method.setAccessible(true);
		method.invoke(triples);

		RecordingSequence recording = triples.recordingSequence();
		assertNotNull("Recording sequence not initialized", recording);
		assertEquals(recordLimit, recording.recordedCount());

		long[] positions = recording.recordedPositions();
		for (int i = 1; i < recordLimit; i++) {
			assertTrue("Expected non-decreasing writes but saw " + positions[i - 1] + " > " + positions[i],
					positions[i - 1] <= positions[i]);
		}

		triples.close();
	}

	private static final class RecordingBitmapTriples extends BitmapTriples {
		private final int recordLimit;
		private RecordingSequence recordingSequence;

		private RecordingBitmapTriples(HDTOptions spec, int recordLimit) throws IOException {
			super(spec);
			this.recordLimit = recordLimit;
		}

		@Override
		public DynamicSequence createSequence64(Path baseDir, String name, int size, long capacity, boolean forceDisk)
				throws IOException {
			if ("objectArray".equals(name)) {
				recordingSequence = new RecordingSequence(size, capacity, recordLimit);
				return recordingSequence;
			}
			return super.createSequence64(baseDir, name, size, capacity, forceDisk);
		}

		private RecordingSequence recordingSequence() {
			return recordingSequence;
		}
	}

	private static final class RecordingSequence extends SequenceLog64Big {
		private final long[] positions;
		private int recorded;

		private RecordingSequence(int numbits, long capacity, int recordLimit) {
			super(numbits, capacity, true);
			this.positions = new long[recordLimit];
		}

		@Override
		public void set(long position, long value) {
			if (recorded < positions.length) {
				positions[recorded++] = position;
			}
			super.set(position, value);
		}

		private long[] recordedPositions() {
			return positions;
		}

		private int recordedCount() {
			return recorded;
		}
	}
}
