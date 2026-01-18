package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public final class PairRange {
	private static final String RANGE_SUFFIX = ".range";

	private PairRange() {
	}

	public static KeyRange<Pair> readRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		CloseSuppressPath rangeFile = rangePath(dataFile);
		if (!Files.exists(rangeFile)) {
			return null;
		}
		try (InputStream in = rangeFile.openInputStream(CloseSuppressPath.BUFFER_SIZE)) {
			Pair min = readPair(in);
			Pair max = readPair(in);
			return new KeyRange<>(min, max);
		}
	}

	public static void writeRange(CloseSuppressPath dataFile, Pair min, Pair max) throws IOException {
		if (min == null || max == null) {
			deleteRangeIfExists(dataFile);
			return;
		}
		CloseSuppressPath rangeFile = rangePath(dataFile);
		try (OutputStream out = rangeFile.openOutputStream(CloseSuppressPath.BUFFER_SIZE)) {
			writePair(out, min);
			writePair(out, max);
		}
	}

	public static void deleteRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		Files.deleteIfExists(rangePath(dataFile));
	}

	private static CloseSuppressPath rangePath(CloseSuppressPath dataFile) {
		return dataFile.resolveSibling(dataFile.getFileName().toString() + RANGE_SUFFIX);
	}

	private static Pair readPair(InputStream in) throws IOException {
		long predicatePosition = VByte.decodeSigned(in);
		long object = VByte.decodeSigned(in);
		long predicate = VByte.decodeSigned(in);
		Pair pair = new Pair();
		pair.setAll(predicatePosition, object, predicate);
		return pair;
	}

	private static void writePair(OutputStream out, Pair pair) throws IOException {
		VByte.encodeSigned(out, pair.predicatePosition);
		VByte.encodeSigned(out, pair.object);
		VByte.encodeSigned(out, pair.predicate);
	}
}
