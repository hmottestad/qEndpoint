package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Bucketed mapping sink for disk imports.
 * <p>
 * Instead of writing tripleId-indexed mapping arrays with random writes during
 * dictionary construction, this implementation appends (offsetInBucket,
 * headerId) records into per-bucket files. After dictionary construction,
 * {@link #materializeTo(CompressTripleMapper)} can be used to populate the
 * standard {@link CompressTripleMapper} mapping files sequentially.
 */
public class BucketedTripleMapper implements CompressFourSectionDictionary.NodeConsumer, Closeable {
	private static final Logger log = LoggerFactory.getLogger(BucketedTripleMapper.class);
	private static final int RECORD_BYTES = Integer.BYTES + Long.BYTES;

	private final RoleSpooler subjects;
	private final RoleSpooler predicates;
	private final RoleSpooler objects;
	private final RoleSpooler graphs;
	private final boolean supportsGraph;

	public BucketedTripleMapper(CloseSuppressPath location, long tripleCount, boolean supportsGraph, int bucketSize,
			int bufferSize) throws IOException {
		this.supportsGraph = supportsGraph;
		location.mkdirs();
		subjects = new RoleSpooler(location.resolve("subjects"), tripleCount, bucketSize, bufferSize);
		predicates = new RoleSpooler(location.resolve("predicates"), tripleCount, bucketSize, bufferSize);
		objects = new RoleSpooler(location.resolve("objects"), tripleCount, bucketSize, bufferSize);
		if (supportsGraph) {
			graphs = new RoleSpooler(location.resolve("graphs"), tripleCount, bucketSize, bufferSize);
		} else {
			graphs = null;
		}
	}

	@Override
	public void onSubject(long preMapId, long newMapId) {
		subjects.add(preMapId, newMapId);
	}

	@Override
	public void onSubject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		subjects.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onPredicate(long preMapId, long newMapId) {
		predicates.add(preMapId, newMapId);
	}

	@Override
	public void onPredicate(long[] preMapIds, long[] newMapIds, int offset, int length) {
		predicates.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onObject(long preMapId, long newMapId) {
		objects.add(preMapId, newMapId);
	}

	@Override
	public void onObject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		objects.add(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onGraph(long preMapId, long newMapId) {
		if (!supportsGraph) {
			return;
		}
		graphs.add(preMapId, newMapId);
	}

	@Override
	public void onGraph(long[] preMapIds, long[] newMapIds, int offset, int length) {
		if (!supportsGraph) {
			return;
		}
		graphs.add(preMapIds, newMapIds, offset, length);
	}

	public void materializeTo(CompressTripleMapper mapper) throws IOException {
		materializeTo(mapper, ProgressListener.ignore());
	}

	public void materializeTo(CompressTripleMapper mapper, ProgressListener listener) throws IOException {
		flush();
		ProgressListener progressListener = ProgressListener.ofNullable(listener);
		subjects.materialize(Role.SUBJECT, mapper, progressListener);
		predicates.materialize(Role.PREDICATE, mapper, progressListener);
		objects.materialize(Role.OBJECT, mapper, progressListener);
		if (supportsGraph) {
			graphs.materialize(Role.GRAPH, mapper, progressListener);
		}
	}

	private void flush() throws IOException {
		subjects.flush();
		predicates.flush();
		objects.flush();
		if (supportsGraph) {
			graphs.flush();
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		subjects.close();
		predicates.close();
		objects.close();
		if (supportsGraph) {
			graphs.close();
		}
	}

	private enum Role {
		SUBJECT, PREDICATE, OBJECT, GRAPH
	}

	private static final class RoleSpooler implements Closeable {
		private static final int IO_BUFFER_BYTES = 1024 * 1024;
		private static final int CHUNK_HEADER_BYTES = Integer.BYTES + Integer.BYTES;
		private final CloseSuppressPath root;
		private final long tripleCount;
		private final int bucketSize;
		private final int bucketCount;

		private final int[] bucketBuffer;
		private final int[] offsetBuffer;
		private final long[] headerBuffer;
		private int size;

		private final int[] bucketCounts;
		private final int[] bucketOffsets;
		private final int[] bucketWriteCursor;
		private final int[] sortedOffsets;
		private final long[] sortedHeaders;
		private final byte[] ioBuffer;
		private final byte[] compressedBuffer;
		private final byte[] chunkHeader;
		private final LZ4Compressor compressor;
		private final LZ4FastDecompressor decompressor;

		private RoleSpooler(CloseSuppressPath root, long tripleCount, int bucketSize, int bufferSize)
				throws IOException {
			if (tripleCount < 0) {
				throw new IllegalArgumentException("Negative tripleCount: " + tripleCount);
			}
			if (bucketSize <= 0) {
				throw new IllegalArgumentException("bucketSize must be > 0");
			}
			if (bufferSize <= 0) {
				throw new IllegalArgumentException("bufferSize must be > 0");
			}
			this.root = root;
			this.tripleCount = tripleCount;
			this.bucketSize = bucketSize;
			this.bucketCount = (int) Math.max(1, (tripleCount + bucketSize - 1) / bucketSize);

			root.mkdirs();

			bucketBuffer = new int[bufferSize];
			offsetBuffer = new int[bufferSize];
			headerBuffer = new long[bufferSize];

			bucketCounts = new int[bucketCount];
			bucketOffsets = new int[bucketCount + 1];
			bucketWriteCursor = new int[bucketCount];
			sortedOffsets = new int[bufferSize];
			sortedHeaders = new long[bufferSize];

			LZ4Factory factory = LZ4Factory.fastestInstance();
			compressor = factory.fastCompressor();
			decompressor = factory.fastDecompressor();
			ioBuffer = new byte[IO_BUFFER_BYTES];
			compressedBuffer = new byte[compressor.maxCompressedLength(IO_BUFFER_BYTES)];
			chunkHeader = new byte[CHUNK_HEADER_BYTES];
		}

		private void add(long tripleId, long headerId) {
			if (tripleId <= 0) {
				throw new IllegalArgumentException("tripleId must be > 0");
			}
			long value = tripleId - 1;
			int bucket = (int) (value / bucketSize);
			int offset = (int) (value - (long) bucket * bucketSize);
			bucketBuffer[size] = bucket;
			offsetBuffer[size] = offset;
			headerBuffer[size] = headerId;
			size++;
			if (size == bucketBuffer.length) {
				try {
					flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		private void add(long[] tripleIds, long[] headerIds, int offset, int length) {
			for (int i = offset; i < offset + length; i++) {
				add(tripleIds[i], headerIds[i]);
			}
		}

		private void flush() throws IOException {
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
				sortedHeaders[outIndex] = headerBuffer[i];
			}

			for (int bucket = 0; bucket < bucketCount; bucket++) {
				int start = bucketOffsets[bucket];
				int end = bucketOffsets[bucket + 1];
				if (start == end) {
					continue;
				}
				CloseSuppressPath file = root.resolve(bucketFileName(bucket));
				try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE,
						StandardOpenOption.WRITE, StandardOpenOption.APPEND), IO_BUFFER_BYTES)) {
					writeRecords(out, sortedOffsets, sortedHeaders, start, end);
				}
			}

			size = 0;
		}

		private void materialize(Role role, CompressTripleMapper mapper, ProgressListener listener) throws IOException {
			ProgressListener progressListener = ProgressListener.ofNullable(listener);
			if (tripleCount == 0) {
				progressListener.notifyProgress(100, "Materialized {} mapping", role);
				return;
			}
			progressListener.notifyProgress(0, "Materializing {} mapping", role);
			long processed = 0;
			int lastPercent = 0;
			for (int bucket = 0; bucket < bucketCount; bucket++) {
				long bucketStart = (long) bucket * bucketSize + 1;
				long remaining = tripleCount - (bucketStart - 1);
				if (remaining <= 0) {
					break;
				}
				int count = (int) Math.min(bucketSize, remaining);

				long[] headers = new long[count];
				CloseSuppressPath file = root.resolve(bucketFileName(bucket));
				if (!Files.exists(file)) {
					throw new IllegalStateException("Missing bucket file: " + file);
				}
				try (InputStream in = new BufferedInputStream(Files.newInputStream(file), IO_BUFFER_BYTES)) {
					readRecords(in, headers);
				}

				for (int i = 0; i < count; i++) {
					long tripleId = bucketStart + i;
					long headerId = headers[i];
					if (headerId == 0) {
						throw new IllegalStateException("Missing mapping for tripleId=" + tripleId + " (" + role + ")");
					}
					switch (role) {
					case SUBJECT -> mapper.onSubject(tripleId, headerId);
					case PREDICATE -> mapper.onPredicate(tripleId, headerId);
					case OBJECT -> mapper.onObject(tripleId, headerId);
					case GRAPH -> mapper.onGraph(tripleId, headerId);
					default -> throw new IllegalStateException("Unknown role: " + role);
					}
				}
				processed += count;
				int percent = (int) ((processed * 100d) / tripleCount);
				if (percent > lastPercent) {
					progressListener.notifyProgress(percent, "Materializing {} mapping {}/{}", role, processed,
							tripleCount);
					lastPercent = percent;
				}
			}
			progressListener.notifyProgress(100, "Materialized {} mapping", role);
		}

		private void writeRecords(OutputStream out, int[] offsets, long[] headers, int from, int to)
				throws IOException {
			byte[] buffer = ioBuffer;
			int pos = 0;
			for (int i = from; i < to; i++) {
				if (buffer.length - pos < RECORD_BYTES) {
					writeChunk(out, buffer, pos);
					pos = 0;
				}
				putInt(buffer, pos, offsets[i]);
				pos += Integer.BYTES;
				putLong(buffer, pos, headers[i]);
				pos += Long.BYTES;
			}
			if (pos > 0) {
				writeChunk(out, buffer, pos);
			}
		}

		private void readRecords(InputStream in, long[] headersByOffset) throws IOException {
			byte[] header = chunkHeader;
			byte[] buffer = ioBuffer;
			byte[] compressed = compressedBuffer;
			while (true) {
				int headerRead = readFully(in, header, 0, CHUNK_HEADER_BYTES);
				if (headerRead == -1) {
					return;
				}
				if (headerRead < CHUNK_HEADER_BYTES) {
					throw new EOFException(
							"Unexpected end of chunk header: " + headerRead + " < " + CHUNK_HEADER_BYTES);
				}
				int uncompressedLength = getInt(header, 0);
				int compressedLength = getInt(header, Integer.BYTES);
				if (uncompressedLength <= 0 || compressedLength <= 0) {
					throw new EOFException("Unexpected chunk lengths: uncompressed=" + uncompressedLength
							+ " compressed=" + compressedLength);
				}
				if (uncompressedLength > buffer.length) {
					throw new IOException("Chunk too large: " + uncompressedLength + " > " + buffer.length);
				}
				if (compressedLength > compressed.length) {
					throw new IOException(
							"Compressed chunk too large: " + compressedLength + " > " + compressed.length);
				}
				int read = readFully(in, compressed, 0, compressedLength);
				if (read < compressedLength) {
					throw new EOFException("Unexpected end of compressed chunk: " + read + " < " + compressedLength);
				}
				if (uncompressedLength % RECORD_BYTES != 0) {
					throw new EOFException("Unexpected uncompressed chunk length: " + uncompressedLength);
				}
				decompressor.decompress(compressed, 0, buffer, 0, uncompressedLength);
				for (int pos = 0; pos < uncompressedLength; pos += RECORD_BYTES) {
					int offset = getInt(buffer, pos);
					long headerId = getLong(buffer, pos + Integer.BYTES);
					if (offset < 0 || offset >= headersByOffset.length) {
						throw new IOException("Offset out of range: " + offset + " >= " + headersByOffset.length);
					}
					headersByOffset[offset] = headerId;
				}
			}
		}

		private void writeChunk(OutputStream out, byte[] buffer, int length) throws IOException {
			int compressedLength = compressor.compress(buffer, 0, length, compressedBuffer, 0, compressedBuffer.length);
			putInt(chunkHeader, 0, length);
			putInt(chunkHeader, Integer.BYTES, compressedLength);
			out.write(chunkHeader, 0, CHUNK_HEADER_BYTES);
			out.write(compressedBuffer, 0, compressedLength);
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

		private static String bucketFileName(int bucket) {
			if (bucket < 0 || bucket >= 1_000_000) {
				// fallback that preserves the original semantics
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
			c[1] = (char) ('0' + x); // now 0..9

			return new String(c);
		}

		private static final String ZEROS_6 = "000000";

		private static String bucketFileNameSlowButGeneral(int bucket) {
			// same implementation as the “Fast and fully correct” version above
			if (bucket >= 0) {
				String digits = Integer.toString(bucket);
				int pad = 6 - digits.length();
				StringBuilder sb = new StringBuilder(1 + Math.max(6, digits.length()) + 4);
				sb.append('b');
				if (pad > 0)
					sb.append(ZEROS_6, 0, pad);
				sb.append(digits).append(".bin");
				return sb.toString();
			}
			long v = -(long) bucket;
			String digits = Long.toString(v);
			int pad = 5 - digits.length();
			StringBuilder sb = new StringBuilder(1 + 1 + Math.max(5, digits.length()) + 4);
			sb.append('b').append('-');
			if (pad > 0)
				sb.append(ZEROS_6, 0, pad);
			sb.append(digits).append(".bin");
			return sb.toString();
		}

		@Override
		public void close() throws IOException {
			root.closeWithDeleteRecurse();
			try {
				root.close();
			} catch (IOException e) {
				log.warn("Can't delete bucketed mapping directory {}", root, e);
			}
		}
	}

}
