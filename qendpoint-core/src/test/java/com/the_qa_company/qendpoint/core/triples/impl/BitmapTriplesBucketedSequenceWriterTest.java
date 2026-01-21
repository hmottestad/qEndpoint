package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitmapTriplesBucketedSequenceWriterTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void bucketFilesUseLz4Chunks() throws Exception {
		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				writer.add(0L, 5L);
				writer.add(1L, 7L);

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence);

				Path bucketFile;
				try (Stream<Path> paths = Files.list(root)) {
					bucketFile = paths.filter(Files::isRegularFile).findFirst()
							.orElseThrow(() -> new AssertionError("bucket files written"));
				}

				byte[] header = new byte[8];
				try (InputStream in = Files.newInputStream(bucketFile)) {
					int read = in.read(header);
					assertEquals("chunk header length", 8, read);
				}
				int uncompressedLength = readInt(header, 0);
				int compressedLength = readInt(header, Integer.BYTES);
				int expectedUncompressedLength = (Integer.BYTES + Long.BYTES) * 2;
				assertEquals("chunk uncompressed length", expectedUncompressedLength, uncompressedLength);
				assertTrue("chunk compressed length positive", compressedLength > 0);
			} finally {
				writer.close();
			}
		}
	}

	@Test
	public void materializeReportsProgress() throws Exception {
		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				writer.add(0L, 5L);
				writer.add(1L, 7L);

				List<Float> levels = new ArrayList<>();
				List<String> messages = new ArrayList<>();
				ProgressListener listener = (level, message) -> {
					levels.add(level);
					messages.add(message);
				};

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence, listener);

				assertTrue("progress listener invoked", !messages.isEmpty());
				assertTrue("progress reached 100", levels.stream().anyMatch(level -> level >= 100f));
			} finally {
				writer.close();
			}
		}
	}

	@Test
	public void addBatchWritesRecords() throws Exception {
		long totalEntries = 4L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			BucketedSequenceWriter writer = new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
			try {
				long[] indexes = { 0L, 1L, 2L, 3L };
				long[] values = { 9L, 7L, 5L, 3L };
				writer.addBatch(indexes, values, indexes.length);

				DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
				writer.materializeTo(sequence);

				assertEquals("value 0", 9L, sequence.get(0));
				assertEquals("value 1", 7L, sequence.get(1));
				assertEquals("value 2", 5L, sequence.get(2));
				assertEquals("value 3", 3L, sequence.get(3));
			} finally {
				writer.close();
			}
		}
	}

	private static int readInt(byte[] buffer, int offset) {
		return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
				| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
	}
}
