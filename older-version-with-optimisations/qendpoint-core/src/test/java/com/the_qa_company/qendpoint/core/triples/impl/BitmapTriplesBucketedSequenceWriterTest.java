package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
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
		Class<?> writerClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$BucketedSequenceWriter");
		Constructor<?> constructor = writerClass.getDeclaredConstructor(CloseSuppressPath.class, long.class, int.class,
				int.class);
		constructor.setAccessible(true);

		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			Object writer = constructor.newInstance(root, totalEntries, bucketSize, bufferRecords);
			Method add = writerClass.getDeclaredMethod("add", long.class, long.class);
			add.setAccessible(true);
			add.invoke(writer, 0L, 5L);
			add.invoke(writer, 1L, 7L);

			DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
			Method materializeTo = writerClass.getDeclaredMethod("materializeTo", DynamicSequence.class);
			materializeTo.setAccessible(true);
			materializeTo.invoke(writer, sequence);

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

			Method close = writerClass.getDeclaredMethod("close");
			close.setAccessible(true);
			close.invoke(writer);
		}
	}

	@Test
	public void materializeReportsProgress() throws Exception {
		Class<?> writerClass = Class
				.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$BucketedSequenceWriter");
		Constructor<?> constructor = writerClass.getDeclaredConstructor(CloseSuppressPath.class, long.class, int.class,
				int.class);
		constructor.setAccessible(true);

		long totalEntries = 2L;
		int bucketSize = 2;
		int bufferRecords = 4;

		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			Object writer = constructor.newInstance(root, totalEntries, bucketSize, bufferRecords);
			Method add = writerClass.getDeclaredMethod("add", long.class, long.class);
			add.setAccessible(true);
			add.invoke(writer, 0L, 5L);
			add.invoke(writer, 1L, 7L);

			List<Float> levels = new ArrayList<>();
			List<String> messages = new ArrayList<>();
			ProgressListener listener = (level, message) -> {
				levels.add(level);
				messages.add(message);
			};

			DynamicSequence sequence = new SequenceLog64(64, totalEntries, true);
			Method materializeTo = writerClass.getDeclaredMethod("materializeTo", DynamicSequence.class,
					ProgressListener.class);
			materializeTo.setAccessible(true);
			materializeTo.invoke(writer, sequence, listener);

			assertTrue("progress listener invoked", !messages.isEmpty());
			assertTrue("progress reached 100", levels.stream().anyMatch(level -> level >= 100f));

			Method close = writerClass.getDeclaredMethod("close");
			close.setAccessible(true);
			close.invoke(writer);
		}
	}

	private static int readInt(byte[] buffer, int offset) {
		return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
				| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
	}
}
