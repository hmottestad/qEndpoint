package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public final class CompressTripleRange {
	private static final String RANGE_SUFFIX = ".range";

	private CompressTripleRange() {
	}

	public static KeyRange<TripleID> readRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		CloseSuppressPath rangeFile = rangePath(dataFile);
		if (!Files.exists(rangeFile)) {
			return null;
		}
		try (InputStream in = rangeFile.openInputStream(CloseSuppressPath.BUFFER_SIZE)) {
			int flags = in.read();
			if (flags == -1) {
				throw new IOException("Range file missing header: " + rangeFile);
			}
			boolean quad = (flags & CompressTripleWriter.FLAG_QUAD) != 0;
			TripleID min = readTriple(in, quad);
			TripleID max = readTriple(in, quad);
			return new KeyRange<>(min, max);
		}
	}

	public static void writeRange(CloseSuppressPath dataFile, TripleID min, TripleID max, boolean quad)
			throws IOException {
		if (min == null || max == null) {
			deleteRangeIfExists(dataFile);
			return;
		}
		CloseSuppressPath rangeFile = rangePath(dataFile);
		try (OutputStream out = rangeFile.openOutputStream(CloseSuppressPath.BUFFER_SIZE)) {
			int flags = quad ? CompressTripleWriter.FLAG_QUAD : 0;
			out.write(flags);
			writeTriple(out, min, quad);
			writeTriple(out, max, quad);
		}
	}

	public static void deleteRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		Files.deleteIfExists(rangePath(dataFile));
	}

	private static CloseSuppressPath rangePath(CloseSuppressPath dataFile) {
		return dataFile.resolveSibling(dataFile.getFileName().toString() + RANGE_SUFFIX);
	}

	private static void writeTriple(OutputStream out, TripleID triple, boolean quad) throws IOException {
		VByte.encode(out, triple.getSubject());
		VByte.encode(out, triple.getPredicate());
		VByte.encode(out, triple.getObject());
		if (quad) {
			VByte.encode(out, triple.getGraph());
		}
	}

	private static TripleID readTriple(InputStream in, boolean quad) throws IOException {
		long s = VByte.decode(in);
		long p = VByte.decode(in);
		long o = VByte.decode(in);
		if (quad) {
			long g = VByte.decode(in);
			return new TripleID(s, p, o, g);
		}
		return new TripleID(s, p, o);
	}
}
