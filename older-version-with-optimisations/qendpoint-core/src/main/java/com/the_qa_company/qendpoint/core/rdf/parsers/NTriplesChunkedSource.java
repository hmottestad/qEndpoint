package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chunk factory for N-Triples / N-Quads. Pull-based end-to-end: a KWay worker
 * asks for a chunk supplier (this.get()). Stream-backed instances read lines
 * under a small lock, while {@link Path}-backed instances memory-map the file
 * and hand out zero-copy byte slices. Both paths yield
 * {@link TripleString}/{@link QuadString} via {@link SizedSupplier#get()}.
 */
public final class NTriplesChunkedSource
		implements ExceptionSupplier<SizedSupplier<TripleString>, IOException>, Closeable {

	private static final Logger log = LoggerFactory.getLogger(NTriplesChunkedSource.class);
	private static final byte LF = (byte) '\n';
	private static final byte CR = (byte) '\r';
	private static final byte BS = (byte) '\\';

	private final boolean mmapMode;

	private final BufferedReader reader;
	private final Object lock = new Object();

	private final MemorySegment mapped;
	private final Arena arena;
	private final FileChannel channel;
	private final long fileSize;
	private final AtomicLong nextOffset;
	private final long probeStep;

	private final boolean readQuad;
	private final long chunkBudgetBytes;

	// limits how much we buffer at once (prevents "double buffering" the whole
	// chunk in RAM)
	private final long maxBatchBytes;
	private final int maxBatchLines;

	private volatile boolean eof;

	public NTriplesChunkedSource(InputStream input, RDFNotation notation, long chunkBudgetBytes) {
		this(input, notation, chunkBudgetBytes, 8L * 1024 * 1024, 8192);
	}

	public NTriplesChunkedSource(InputStream input, RDFNotation notation, long chunkBudgetBytes, long maxBatchBytes,
			int maxBatchLines) {
		Objects.requireNonNull(input, "input");
		Objects.requireNonNull(notation, "notation");

		if (notation != RDFNotation.NTRIPLES && notation != RDFNotation.NQUAD) {
			throw new IllegalArgumentException("NTriplesChunkedSource supports only NTRIPLES/NQUAD, got " + notation);
		}
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}
		if (maxBatchBytes <= 0) {
			throw new IllegalArgumentException("maxBatchBytes must be > 0");
		}
		if (maxBatchLines <= 0) {
			throw new IllegalArgumentException("maxBatchLines must be > 0");
		}

		this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
		this.readQuad = notation == RDFNotation.NQUAD;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.maxBatchBytes = maxBatchBytes;
		this.maxBatchLines = maxBatchLines;
		this.mmapMode = false;
		this.mapped = null;
		this.arena = null;
		this.channel = null;
		this.fileSize = 0L;
		this.nextOffset = null;
		this.probeStep = 0L;
	}

	public NTriplesChunkedSource(Path path, RDFNotation notation, long chunkBudgetBytes) throws IOException {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(notation, "notation");

		if (notation != RDFNotation.NTRIPLES && notation != RDFNotation.NQUAD) {
			throw new IllegalArgumentException("NTriplesChunkedSource supports only NTRIPLES/NQUAD, got " + notation);
		}
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}

		this.reader = null;
		this.mmapMode = true;
		this.readQuad = notation == RDFNotation.NQUAD;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.maxBatchBytes = 0L;
		this.maxBatchLines = 0;
		this.nextOffset = new AtomicLong(0L);
		this.probeStep = Math.max(1L, chunkBudgetBytes / 8L);

		FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
		Arena sharedArena = Arena.ofShared();
		MemorySegment mappedSegment;
		long size;
		try {
			size = ch.size();
			mappedSegment = ch.map(FileChannel.MapMode.READ_ONLY, 0L, size, sharedArena);
		} catch (IOException e) {
			try {
				ch.close();
			} finally {
				sharedArena.close();
			}
			throw e;
		}

		this.channel = ch;
		this.arena = sharedArena;
		this.mapped = mappedSegment;
		this.fileSize = size;
	}

	/**
	 * @return next chunk supplier, or null when input is exhausted.
	 */
	@Override
	public SizedSupplier<TripleString> get() throws IOException {
		if (mmapMode) {
			return nextMappedChunk();
		}
		// Prefetch a first non-null line so we don't create empty chunks at
		// EOF.
		final String firstLine;
		synchronized (lock) {
			if (eof) {
				return null;
			}

			String line;
			do {
				line = reader.readLine();
				if (line == null) {
					eof = true;
					return null;
				}
			} while (line.isEmpty());

			firstLine = line;
		}

		return new Chunk(firstLine);
	}

	private SizedSupplier<TripleString> nextMappedChunk() {
		while (true) {
			long start = nextOffset.get();
			if (start >= fileSize) {
				return null;
			}
			long probe = Math.min(start + probeStep, fileSize);
			long end = findNextUnescapedLineBreakEnd(mapped, probe, fileSize);
			if (end <= start) {
				end = fileSize;
			}
			if (nextOffset.compareAndSet(start, end)) {
				return new MmapChunk(start, end);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (mmapMode) {
			try {
				arena.close();
			} finally {
				channel.close();
			}
			return;
		}
		synchronized (lock) {
			eof = true;
		}
		reader.close();
	}

	private final class Chunk implements SizedSupplier<TripleString> {
		private final ArrayList<String> lineBuffer = new ArrayList<>(1024);
		private int idx;

		private long remaining = chunkBudgetBytes;
		private long size;
		private boolean done;

		private final TripleString reusable = readQuad ? new QuadString() : new TripleString();

		private Chunk(String firstLine) {
			lineBuffer.add(firstLine);
			long est = estimateLineSize(firstLine);
			size += est;
			remaining -= est;
		}

		@Override
		public TripleString get() {
			if (done) {
				return null;
			}

			try {
				while (true) {
					// Need more lines?
					if (idx >= lineBuffer.size()) {
						lineBuffer.clear();
						idx = 0;

						// stop condition for this chunk
						if (remaining <= 0) {
							done = true;
							return null;
						}

						fillBuffer();
						if (lineBuffer.isEmpty()) {
							done = true;
							return null;
						}
					}

					String line = lineBuffer.get(idx++);
					if (parseLine(line, reusable, readQuad)) {
						return reusable;
					}
					// skip comments/blank/invalid lines, keep scanning
				}
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		@Override
		public long getSize() {
			return size;
		}

		private void fillBuffer() throws IOException {
			// cap in-memory buffering
			long batchBudget = Math.min(remaining, maxBatchBytes);

			int count = 0;
			synchronized (lock) {
				while (!eof && count < maxBatchLines && batchBudget > 0) {
					String line = reader.readLine();
					if (line == null) {
						eof = true;
						break;
					}
					if (line.isEmpty()) {
						continue;
					}

					lineBuffer.add(line);

					long est = estimateLineSize(line);
					size += est;
					remaining -= est;
					batchBudget -= est;

					count++;
				}
			}
		}
	}

	private final class MmapChunk implements SizedSupplier<TripleString> {
		private final long end;
		private final long size;
		private long cursor;
		private boolean done;

		private final TripleString reusable = readQuad ? new QuadString() : new TripleString();

		private MmapChunk(long start, long end) {
			this.cursor = start;
			this.end = end;
			this.size = end - start;
		}

		@Override
		public TripleString get() {
			if (done) {
				return null;
			}

			while (cursor < end) {
				long lineStart = cursor;
				long lineBreak = findNextLineBreak(mapped, lineStart, end);
				long lineEndExclusive;
				if (lineBreak < end) {
					lineEndExclusive = lineBreak;
					if (lineEndExclusive > lineStart && byteAt(mapped, lineEndExclusive - 1) == CR) {
						lineEndExclusive--;
					}
					cursor = lineBreak + 1;
				} else {
					lineEndExclusive = end;
					if (lineEndExclusive > lineStart && byteAt(mapped, lineEndExclusive - 1) == CR) {
						lineEndExclusive--;
					}
					cursor = end;
				}

				if (lineEndExclusive <= lineStart) {
					continue;
				}

				if (parseLine(mapped, lineStart, lineEndExclusive - 1, reusable, readQuad)) {
					return reusable;
				}
				// skip comments/blank/invalid lines, keep scanning
			}

			done = true;
			return null;
		}

		@Override
		public long getSize() {
			return size;
		}
	}

	private static long estimateLineSize(String line) {
		// Simple but stable: approximates bytes consumed in the NT stream.
		// (N-Triples is overwhelmingly ASCII; for stats/chunking, char-length
		// is fine.)
		return (long) line.length() + 1L; // + '\n'
	}

	/**
	 * Mirrors {@link RDFParserSimple}'s whitespace trimming and comment
	 * skipping.
	 */
	private static boolean parseLine(String line, TripleString out, boolean readQuad) {
		int start = 0;
		while (start < line.length()) {
			char c = line.charAt(start);
			if (c != ' ' && c != '\t') {
				break;
			}
			start++;
		}

		int end = line.length() - 1;
		while (end >= 0) {
			char c = line.charAt(end);
			if (c != ' ' && c != '\t') {
				break;
			}
			end--;
		}

		if (start + 1 >= end) {
			return false;
		}
		if (line.charAt(start) == '#') {
			return false;
		}

		try {
			out.readByteString(line, start, end, readQuad);
			if (!out.hasEmpty()) {
				return true;
			}

			log.warn("Could not parse triple, ignored.\n{}", line);
			return false;
		} catch (Exception e) {
			log.warn("Could not parse triple, ignored.\n{}", line);
			return false;
		}
	}

	private static boolean parseLine(MemorySegment segment, long start, long end, TripleString out, boolean readQuad) {
		long s = start;
		while (s <= end) {
			byte b = byteAt(segment, s);
			if (b != ' ' && b != '\t') {
				break;
			}
			s++;
		}

		long e = end;
		while (e >= s) {
			byte b = byteAt(segment, e);
			if (b != ' ' && b != '\t') {
				break;
			}
			e--;
		}

		if (s + 1 >= e) {
			return false;
		}
		if (byteAt(segment, s) == '#') {
			return false;
		}

		try {
			readByteString(segment, s, e, readQuad, out);
			if (!out.hasEmpty()) {
				return true;
			}

			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(segment, s, e));
			}
			return false;
		} catch (Exception e1) {
			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(segment, s, e));
			}
			return false;
		}
	}

	private static void readByteString(MemorySegment segment, long start, long endInclusive, boolean processQuad,
			TripleString out) throws ParserException {
		long end = endInclusive + 1;
		out.clear();

		long split = searchNextTabOrSpace(segment, start, end);
		if (split == -1) {
			return;
		}

		long posa = start;
		long posb = split;

		if (byteAt(segment, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(segment, posb - 1) == '>') {
				posb--;
			}
		}
		out.setSubject(UnicodeEscape.unescapeByteString(segment, posa, posb));

		posa = split + 1;
		split = searchNextTabOrSpace(segment, posa, end);
		if (split == -1) {
			return;
		}
		posb = split;

		if (byteAt(segment, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(segment, posb - 1) == '>') {
				posb--;
			}
		}
		out.setPredicate(UnicodeEscape.unescapeByteString(segment, posa, posb));

		posa = split + 1;
		posb = end;

		if (posb <= posa) {
			return;
		}
		if (byteAt(segment, posb - 1) == '.') {
			posb--;
		}
		if (posb <= posa) {
			return;
		}
		byte prev = byteAt(segment, posb - 1);
		if (prev == ' ' || prev == '\t') {
			posb--;
		}
		if (posb <= posa) {
			return;
		}

		if (processQuad) {
			byte lastElem = byteAt(segment, posb - 1);
			if (lastElem != '"') {
				if (lastElem == '>') {
					long iriStart = lastIndexOf(segment, (byte) '<', posb - 1, posa);
					if (iriStart < posa) {
						throw new ParserException("end of a '>' without a start '<'");
					}
					if (posa != iriStart && iriStart > 0 && byteAt(segment, iriStart - 1) != '^') {
						out.setGraph(UnicodeEscape.unescapeByteString(segment, iriStart + 1, posb - 1));
						posb = iriStart - 1;
					}
				} else {
					long bnodeStart = searchBNodeBackward(segment, posa, posb);
					if (bnodeStart > posa) {
						out.setGraph(UnicodeEscape.unescapeByteString(segment, bnodeStart + 1, posb - 1));
						posb = bnodeStart;
					}
				}
			}
		}

		if (byteAt(segment, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(segment, posb - 1) == '>') {
				posb--;
			}
		}
		out.setObject(UnicodeEscape.unescapeByteString(segment, posa, posb));
	}

	private static long searchNextTabOrSpace(MemorySegment segment, long start, long end) {
		for (long i = start; i < end; i++) {
			byte b = byteAt(segment, i);
			if (b == ' ' || b == '\t') {
				return i;
			}
		}
		return -1L;
	}

	private static long searchBNodeBackward(MemorySegment segment, long start, long end) {
		long loc = end;

		while (start > loc) {
			byte b = byteAt(segment, loc);
			switch (b) {
			case ' ', '\t' -> {
				if (loc + 2 > end) {
					return -1;
				}
				if (byteAt(segment, loc + 1) == '_' && byteAt(segment, loc + 2) == ':') {
					return loc + 1;
				}
			}
			case '^', '@', '>', '<', '"' -> {
				return -1;
			}
			default -> {
			}
			}
			loc--;
		}
		return -1;
	}

	private static long lastIndexOf(MemorySegment segment, byte value, long from, long min) {
		long i = from;
		while (i >= min) {
			if (byteAt(segment, i) == value) {
				return i;
			}
			i--;
		}
		return -1L;
	}

	private static long findNextLineBreak(MemorySegment segment, long from, long end) {
		for (long i = from; i < end; i++) {
			byte b = byteAt(segment, i);
			if (b == LF) {
				return i;
			}
			if (b == CR) {
				if (i + 1 < end && byteAt(segment, i + 1) == LF) {
					return i + 1;
				}
				return i;
			}
		}
		return end;
	}

	private static long findNextUnescapedLineBreakEnd(MemorySegment segment, long from, long fileSize) {
		long i = from;
		while (i < fileSize) {
			if (byteAt(segment, i) == LF) {
				long newlineStart = i;
				if (i > 0 && byteAt(segment, i - 1) == CR) {
					newlineStart = i - 1;
				}
				if (!isEscaped(segment, newlineStart)) {
					return i + 1;
				}
			}
			i++;
		}
		return fileSize;
	}

	private static boolean isEscaped(MemorySegment segment, long newlineStart) {
		long j = newlineStart - 1;
		int backslashes = 0;
		while (j >= 0 && byteAt(segment, j) == BS) {
			backslashes++;
			j--;
		}
		return (backslashes & 1) == 1;
	}

	private static byte byteAt(MemorySegment segment, long offset) {
		return segment.get(ValueLayout.JAVA_BYTE, offset);
	}

	private static String segmentToString(MemorySegment segment, long start, long endInclusive) {
		int len = Math.toIntExact(endInclusive - start + 1);
		byte[] data = new byte[len];
		MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, start, data, 0, len);
		return new String(data, StandardCharsets.UTF_8);
	}
}
