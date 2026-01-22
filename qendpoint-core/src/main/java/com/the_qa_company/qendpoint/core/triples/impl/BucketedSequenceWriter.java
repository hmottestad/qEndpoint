/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /impl/BucketedSequenceWriter.java $ Revision: $Rev: 203 $ Last modified:
 * $Date: 2013-05-24 10:48:53 +0100 (vie, 24 may 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.sequence.DynamicSequence;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

final class BucketedSequenceWriter implements Closeable {
	private static final int OBJECT_INDEX_BUCKET_SIZE = 1 << 20;
	private static final int OBJECT_INDEX_BUFFER_RECORDS = 1 << 20;
	private static final int OBJECT_INDEX_IO_BUFFER_BYTES = 1024 * 1024;
	private static final int OBJECT_INDEX_RECORD_BYTES = Integer.BYTES + Long.BYTES;
	private static final int OBJECT_INDEX_CHUNK_HEADER_BYTES = Integer.BYTES + Integer.BYTES;
	private static final long OBJECT_INDEX_MISSING = -1L;
	private static final int PARALLEL_WRITE_MIN_BUCKETS = 2;
	private static final int PARALLEL_WRITE_MIN_RECORDS = 1 << 14;
	private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
	private static final ThreadLocal<WriteBuffers> WRITE_BUFFERS = ThreadLocal.withInitial(WriteBuffers::new);
	private static volatile BucketWriteObserver bucketWriteObserver;

	interface BucketWriteObserver {
		void onBucketWrite(int bucket, String threadName);
	}

	static void setBucketWriteObserver(BucketWriteObserver observer) {
		bucketWriteObserver = observer;
	}

	private final CloseSuppressPath root;
	private final long totalEntries;
	private final int bucketSize;
	private final int bucketCount;

	private final int[] bucketBuffer;
	private final int[] offsetBuffer;
	private final long[] valueBuffer;
	private int size;

	private final int[] bucketCounts;
	private final int[] bucketOffsets;
	private final int[] bucketWriteCursor;
	private final int[] sortedOffsets;
	private final long[] sortedValues;
	private final byte[] ioBuffer;
	private final byte[] compressedBuffer;
	private final byte[] chunkHeader;
	private final LZ4Compressor compressor;
	private final LZ4FastDecompressor decompressor;
	private int writeParallelism = 1;

	BucketedSequenceWriter(CloseSuppressPath root, long totalEntries, int bucketSize, int bufferRecords) {
		if (totalEntries < 0) {
			throw new IllegalArgumentException("totalEntries must be >= 0");
		}
		if (bucketSize <= 0) {
			throw new IllegalArgumentException("bucketSize must be > 0");
		}
		if (bufferRecords <= 0) {
			throw new IllegalArgumentException("bufferRecords must be > 0");
		}
		this.root = root;
		this.totalEntries = totalEntries;
		this.bucketSize = bucketSize;
		long bucketCountLong = Math.max(1L, (totalEntries + bucketSize - 1L) / bucketSize);
		if (bucketCountLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("bucketCount exceeds integer range: " + bucketCountLong);
		}
		this.bucketCount = (int) bucketCountLong;

		bucketBuffer = new int[bufferRecords];
		offsetBuffer = new int[bufferRecords];
		valueBuffer = new long[bufferRecords];

		bucketCounts = new int[bucketCount];
		bucketOffsets = new int[bucketCount + 1];
		bucketWriteCursor = new int[bucketCount];
		sortedOffsets = new int[bufferRecords];
		sortedValues = new long[bufferRecords];

		compressor = LZ4_FACTORY.fastCompressor();
		decompressor = LZ4_FACTORY.fastDecompressor();
		ioBuffer = new byte[OBJECT_INDEX_IO_BUFFER_BYTES];
		compressedBuffer = new byte[compressor.maxCompressedLength(OBJECT_INDEX_IO_BUFFER_BYTES)];
		chunkHeader = new byte[OBJECT_INDEX_CHUNK_HEADER_BYTES];
	}

	static BucketedSequenceWriter create(Path baseDir, String prefix, long totalEntries, int bucketSize,
			int bufferRecords) throws IOException {

		bucketSize = Math.max(bucketSize, 16 * 1024 * 1024);
		bufferRecords = Math.max(bufferRecords, 1024 * 1024);

		Path rootPath;
		if (baseDir == null) {
			rootPath = Files.createTempDirectory(prefix);
		} else {
			rootPath = Files.createTempDirectory(baseDir, prefix + "-");
		}
		CloseSuppressPath root = CloseSuppressPath.of(rootPath);
		root.closeWithDeleteRecurse();
		return new BucketedSequenceWriter(root, totalEntries, bucketSize, bufferRecords);
	}

	static int resolveObjectIndexBucketSize(long totalEntries) {
		long bucketSize = Math.max(1, Math.min(totalEntries, OBJECT_INDEX_BUCKET_SIZE));
		if (bucketSize > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) bucketSize;
	}

	static int resolveObjectIndexBufferRecords(long totalEntries, int bucketSize) {
		long records = Math.max(1, Math.min(totalEntries, Math.min(bucketSize, OBJECT_INDEX_BUFFER_RECORDS)));
		if (records > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) records;
	}

	void setWriteParallelism(int parallelism) {
		if (parallelism < 1 || parallelism >= Integer.MAX_VALUE - 5) {
			throw new IllegalArgumentException("parallelism must be positive: " + parallelism);
		}
		this.writeParallelism = parallelism;
	}

	void add(long index, long value) {
		if (index < 0 || index >= totalEntries) {
			throw new IndexOutOfBoundsException("index out of bounds: " + index);
		}
		long bucketLong = index / bucketSize;
		int bucket = (int) bucketLong;
		int offset = (int) (index - (long) bucket * bucketSize);
		bucketBuffer[size] = bucket;
		offsetBuffer[size] = offset;
		valueBuffer[size] = value;
		size++;
		if (size == bucketBuffer.length) {
			try {
				flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	void addBatch(long[] indexes, long[] values, int length) {
		if (length <= 0) {
			return;
		}
		if (indexes == null || values == null) {
			throw new NullPointerException();
		}
		if (length > indexes.length || length > values.length) {
			throw new IndexOutOfBoundsException();
		}

		for (int i = 0; i < length; i++) {
			long index = indexes[i];
			if (index < 0 || index >= totalEntries) {
				throw new IndexOutOfBoundsException("index out of bounds: " + index);
			}
			long bucketLong = index / bucketSize;
			int bucket = (int) bucketLong;
			int offset = (int) (index - (long) bucket * bucketSize);
			bucketBuffer[size] = bucket;
			offsetBuffer[size] = offset;
			valueBuffer[size] = values[i];
			size++;
			if (size == bucketBuffer.length) {
				try {
					flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	void flush() throws IOException {
		if (size == 0) {
			return;
		}

		Arrays.fill(bucketCounts, 0);
		for (int i = 0; i < size; i++) {
			bucketCounts[bucketBuffer[i]]++;
		}

		bucketOffsets[0] = 0;
		for (int b = 0; b < bucketCount; b++) {
			bucketOffsets[b + 1] = bucketOffsets[b] + bucketCounts[b];
		}
		System.arraycopy(bucketOffsets, 0, bucketWriteCursor, 0, bucketCount);

		for (int i = 0; i < size; i++) {
			int bucket = bucketBuffer[i];
			int outIndex = bucketWriteCursor[bucket]++;
			sortedOffsets[outIndex] = offsetBuffer[i];
			sortedValues[outIndex] = valueBuffer[i];
		}

		if (shouldWriteInParallel()) {
			writeBucketsParallel();
		} else {
			for (int bucket = 0; bucket < bucketCount; bucket++) {
				int start = bucketOffsets[bucket];
				int end = bucketOffsets[bucket + 1];
				if (start == end) {
					continue;
				}
				CloseSuppressPath file = root.resolve(bucketFileName(bucket));
				try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE,
						StandardOpenOption.WRITE, StandardOpenOption.APPEND))) {
					writeRecords(out, sortedOffsets, sortedValues, start, end);
				}
			}
		}

		size = 0;
	}

	void materializeTo(DynamicSequence sequence) throws IOException {
		materializeTo(sequence, ProgressListener.ignore());
	}

	void materializeTo(DynamicSequence sequence, ProgressListener listener) throws IOException {
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		flush();
		if (totalEntries == 0) {
			progressListener.notifyProgress(100, "Materialized object index");
			return;
		}
		long processed = 0;
		int lastPercent = 0;
		for (int bucket = 0; bucket < bucketCount; bucket++) {
			long bucketStart = (long) bucket * bucketSize;
			long remaining = totalEntries - bucketStart;
			if (remaining <= 0) {
				return;
			}
			int count = (int) Math.min(bucketSize, remaining);
			long[] values = new long[count];
			Arrays.fill(values, OBJECT_INDEX_MISSING);
			CloseSuppressPath file = root.resolve(bucketFileName(bucket));
			if (!Files.exists(file)) {
				throw new IllegalStateException("Missing bucket file: " + file);
			}
			try (InputStream in = new BufferedInputStream(Files.newInputStream(file), OBJECT_INDEX_IO_BUFFER_BYTES)) {
				readRecords(in, values);
			}

			for (int i = 0; i < count; i++) {
				long value = values[i];
				if (value == OBJECT_INDEX_MISSING) {
					throw new IllegalStateException("Missing mapping for index=" + (bucketStart + i));
				}
				sequence.set(bucketStart + i, value);
			}
			processed += count;
			int percent = (int) ((processed * 100d) / totalEntries);
			if (percent > lastPercent) {
				progressListener.notifyProgress(percent,
						"Materializing object index " + processed + "/" + totalEntries);
				lastPercent = percent;
			}
		}
		progressListener.notifyProgress(100, "Materialized object index");
	}

	private void writeRecords(OutputStream out, int[] offsets, long[] values, int from, int to) throws IOException {
		byte[] buffer = ioBuffer;
		int pos = 0;
		for (int i = from; i < to; i++) {
			if (buffer.length - pos < OBJECT_INDEX_RECORD_BYTES) {
				writeChunk(out, buffer, pos);
				pos = 0;
			}
			putInt(buffer, pos, offsets[i]);
			pos += Integer.BYTES;
			putLong(buffer, pos, values[i]);
			pos += Long.BYTES;
		}
		if (pos > 0) {
			writeChunk(out, buffer, pos);
		}
	}

	private void writeRecords(OutputStream out, int[] offsets, long[] values, int from, int to, WriteBuffers buffers)
			throws IOException {
		byte[] buffer = buffers.ioBuffer;
		int pos = 0;
		for (int i = from; i < to; i++) {
			if (buffer.length - pos < OBJECT_INDEX_RECORD_BYTES) {
				writeChunk(out, buffer, pos, buffers);
				pos = 0;
			}
			putInt(buffer, pos, offsets[i]);
			pos += Integer.BYTES;
			putLong(buffer, pos, values[i]);
			pos += Long.BYTES;
		}
		if (pos > 0) {
			writeChunk(out, buffer, pos, buffers);
		}
	}

	private void readRecords(InputStream in, long[] valuesByOffset) throws IOException {
		byte[] header = chunkHeader;
		byte[] buffer = ioBuffer;
		byte[] compressed = compressedBuffer;
		while (true) {
			int headerRead = readFully(in, header, 0, OBJECT_INDEX_CHUNK_HEADER_BYTES);
			if (headerRead == -1) {
				return;
			}
			if (headerRead < OBJECT_INDEX_CHUNK_HEADER_BYTES) {
				throw new EOFException(
						"Unexpected end of chunk header: " + headerRead + " < " + OBJECT_INDEX_CHUNK_HEADER_BYTES);
			}
			int uncompressedLength = getInt(header, 0);
			int compressedLength = getInt(header, Integer.BYTES);
			if (uncompressedLength <= 0 || compressedLength <= 0) {
				throw new EOFException("Unexpected chunk lengths: uncompressed=" + uncompressedLength + " compressed="
						+ compressedLength);
			}
			if (uncompressedLength > buffer.length) {
				throw new IOException("Chunk too large: " + uncompressedLength + " > " + buffer.length);
			}
			if (compressedLength > compressed.length) {
				throw new IOException("Compressed chunk too large: " + compressedLength + " > " + compressed.length);
			}
			int read = readFully(in, compressed, 0, compressedLength);
			if (read < compressedLength) {
				throw new EOFException("Unexpected end of compressed chunk: " + read + " < " + compressedLength);
			}
			if (uncompressedLength % OBJECT_INDEX_RECORD_BYTES != 0) {
				throw new EOFException("Unexpected uncompressed chunk length: " + uncompressedLength);
			}
			decompressor.decompress(compressed, 0, buffer, 0, uncompressedLength);
			for (int pos = 0; pos < uncompressedLength; pos += OBJECT_INDEX_RECORD_BYTES) {
				int offset = getInt(buffer, pos);
				long value = getLong(buffer, pos + Integer.BYTES);
				if (offset < 0 || offset >= valuesByOffset.length) {
					throw new IOException("Offset out of range: " + offset + " >= " + valuesByOffset.length);
				}
				if (valuesByOffset[offset] != OBJECT_INDEX_MISSING) {
					throw new IOException("Duplicate mapping for offset " + offset);
				}
				valuesByOffset[offset] = value;
			}
		}
	}

	private void writeChunk(OutputStream out, byte[] buffer, int length) throws IOException {
		int compressedLength = compressor.compress(buffer, 0, length, compressedBuffer, 0, compressedBuffer.length);
		putInt(chunkHeader, 0, length);
		putInt(chunkHeader, Integer.BYTES, compressedLength);
		out.write(chunkHeader, 0, OBJECT_INDEX_CHUNK_HEADER_BYTES);
		out.write(compressedBuffer, 0, compressedLength);
	}

	private void writeChunk(OutputStream out, byte[] buffer, int length, WriteBuffers buffers) throws IOException {
		int compressedLength = buffers.compressor.compress(buffer, 0, length, buffers.compressedBuffer, 0,
				buffers.compressedBuffer.length);
		putInt(buffers.chunkHeader, 0, length);
		putInt(buffers.chunkHeader, Integer.BYTES, compressedLength);
		out.write(buffers.chunkHeader, 0, OBJECT_INDEX_CHUNK_HEADER_BYTES);
		out.write(buffers.compressedBuffer, 0, compressedLength);
	}

	private static int readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
		int total = 0;
		while (total < length) {
			int read = in.read(buffer, offset + total, length - total);
			if (read == -1) {
				return total == 0 ? -1 : total;
			}
			total += read;
		}
		return total;
	}

	private static void putInt(byte[] buffer, int offset, int value) {
		buffer[offset] = (byte) (value >>> 24);
		buffer[offset + 1] = (byte) (value >>> 16);
		buffer[offset + 2] = (byte) (value >>> 8);
		buffer[offset + 3] = (byte) value;
	}

	private static void putLong(byte[] buffer, int offset, long value) {
		buffer[offset] = (byte) (value >>> 56);
		buffer[offset + 1] = (byte) (value >>> 48);
		buffer[offset + 2] = (byte) (value >>> 40);
		buffer[offset + 3] = (byte) (value >>> 32);
		buffer[offset + 4] = (byte) (value >>> 24);
		buffer[offset + 5] = (byte) (value >>> 16);
		buffer[offset + 6] = (byte) (value >>> 8);
		buffer[offset + 7] = (byte) value;
	}

	private static int getInt(byte[] buffer, int offset) {
		return ((buffer[offset] & 0xff) << 24) | ((buffer[offset + 1] & 0xff) << 16)
				| ((buffer[offset + 2] & 0xff) << 8) | (buffer[offset + 3] & 0xff);
	}

	private static long getLong(byte[] buffer, int offset) {
		return ((long) (buffer[offset] & 0xff) << 56) | ((long) (buffer[offset + 1] & 0xff) << 48)
				| ((long) (buffer[offset + 2] & 0xff) << 40) | ((long) (buffer[offset + 3] & 0xff) << 32)
				| ((long) (buffer[offset + 4] & 0xff) << 24) | ((long) (buffer[offset + 5] & 0xff) << 16)
				| ((long) (buffer[offset + 6] & 0xff) << 8) | ((long) (buffer[offset + 7] & 0xff));
	}

	@Override
	public void close() throws IOException {
		root.close();
	}

	private boolean shouldWriteInParallel() {
		return writeParallelism > 1 && bucketCount >= PARALLEL_WRITE_MIN_BUCKETS && size >= PARALLEL_WRITE_MIN_RECORDS;
	}

	private void writeBucketsParallel() throws IOException {
		ForkJoinPool pool = new ForkJoinPool(writeParallelism);
		List<ForkJoinTask<?>> tasks = new ArrayList<>();
		try {
			int taskCount = Math.min(bucketCount, Math.max(1, writeParallelism * 4));
			int chunkSize = (bucketCount + taskCount - 1) / taskCount;
			for (int task = 0; task < taskCount; task++) {
				int start = task * chunkSize;
				int end = Math.min(bucketCount, start + chunkSize);
				if (start >= end) {
					break;
				}
				tasks.add(pool.submit(() -> writeBucketRange(start, end)));
			}

			for (ForkJoinTask<?> task : tasks) {
				try {
					task.join();
				} catch (RuntimeException e) {
					Throwable cause = e.getCause();
					if (cause instanceof UncheckedIOException uio) {
						throw uio.getCause();
					}
					throw e;
				}
			}
		} finally {
			pool.shutdown();
		}
	}

	private void writeBucketRange(int startBucket, int endBucket) {
		for (int bucket = startBucket; bucket < endBucket; bucket++) {
			int start = bucketOffsets[bucket];
			int end = bucketOffsets[bucket + 1];
			if (start == end) {
				continue;
			}
			notifyBucketWrite(bucket);
			CloseSuppressPath file = root.resolve(bucketFileName(bucket));
			try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE,
					StandardOpenOption.WRITE, StandardOpenOption.APPEND), OBJECT_INDEX_IO_BUFFER_BYTES)) {
				writeRecords(out, sortedOffsets, sortedValues, start, end, WRITE_BUFFERS.get());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	private static void notifyBucketWrite(int bucket) {
		BucketWriteObserver observer = bucketWriteObserver;
		if (observer != null) {
			observer.onBucketWrite(bucket, Thread.currentThread().getName());
		}
	}

	private static final class WriteBuffers {
		private final byte[] ioBuffer;
		private final byte[] compressedBuffer;
		private final byte[] chunkHeader;
		private final LZ4Compressor compressor;

		private WriteBuffers() {
			compressor = LZ4_FACTORY.fastCompressor();
			ioBuffer = new byte[OBJECT_INDEX_IO_BUFFER_BYTES];
			compressedBuffer = new byte[compressor.maxCompressedLength(OBJECT_INDEX_IO_BUFFER_BYTES)];
			chunkHeader = new byte[OBJECT_INDEX_CHUNK_HEADER_BYTES];
		}
	}

	private static String bucketFileName(int bucket) {
		if (bucket < 0 || bucket >= 1_000_000) {
			return bucketFileNameSlowButGeneral(bucket);
		}

		char[] c = { 'b', '0', '0', '0', '0', '0', '0', '.', 'b', 'i', 'n' };
		int x = bucket;

		c[6] = (char) ('0' + (x % 10));
		x /= 10;
		c[5] = (char) ('0' + (x % 10));
		x /= 10;
		c[4] = (char) ('0' + (x % 10));
		x /= 10;
		c[3] = (char) ('0' + (x % 10));
		x /= 10;
		c[2] = (char) ('0' + (x % 10));
		x /= 10;
		c[1] = (char) ('0' + x);

		return new String(c);
	}

	private static final String ZEROS_6 = "000000";

	private static String bucketFileNameSlowButGeneral(int bucket) {
		if (bucket >= 0) {
			String digits = Integer.toString(bucket);
			int pad = 6 - digits.length();
			StringBuilder sb = new StringBuilder(1 + Math.max(6, digits.length()) + 4);
			sb.append('b');
			if (pad > 0) {
				sb.append(ZEROS_6, 0, pad);
			}
			sb.append(digits).append(".bin");
			return sb.toString();
		}
		long v = -(long) bucket;
		String digits = Long.toString(v);
		int pad = 5 - digits.length();
		StringBuilder sb = new StringBuilder(1 + 1 + Math.max(5, digits.length()) + 4);
		sb.append('b').append('-');
		if (pad > 0) {
			sb.append(ZEROS_6, 0, pad);
		}
		sb.append(digits).append(".bin");
		return sb.toString();
	}
}
