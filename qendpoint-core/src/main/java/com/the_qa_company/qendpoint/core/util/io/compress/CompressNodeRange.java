package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator.KeyRange;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public final class CompressNodeRange {
	private static final String RANGE_SUFFIX = ".range";

	private CompressNodeRange() {
	}

	public static KeyRange<IndexedNode> readRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		CloseSuppressPath rangeFile = rangePath(dataFile);
		if (!Files.exists(rangeFile)) {
			return null;
		}
		try (InputStream in = rangeFile.openInputStream(CloseSuppressPath.BUFFER_SIZE)) {
			IndexedNode min = readNode(in);
			IndexedNode max = readNode(in);
			return new KeyRange<>(min, max);
		}
	}

	public static void writeRange(CloseSuppressPath dataFile, KeyRange<IndexedNode> range) throws IOException {
		if (range == null) {
			deleteRangeIfExists(dataFile);
			return;
		}
		writeRange(dataFile, range.minInclusive, range.maxInclusive);
	}

	public static void writeRange(CloseSuppressPath dataFile, IndexedNode min, IndexedNode max) throws IOException {
		if (min == null || max == null) {
			deleteRangeIfExists(dataFile);
			return;
		}
		CloseSuppressPath rangeFile = rangePath(dataFile);
		try (OutputStream out = rangeFile.openOutputStream(CloseSuppressPath.BUFFER_SIZE)) {
			writeNode(out, min);
			writeNode(out, max);
		}
	}

	public static void deleteRangeIfExists(CloseSuppressPath dataFile) throws IOException {
		Files.deleteIfExists(rangePath(dataFile));
	}

	private static CloseSuppressPath rangePath(CloseSuppressPath dataFile) {
		return dataFile.resolveSibling(dataFile.getFileName().toString() + RANGE_SUFFIX);
	}

	private static IndexedNode readNode(InputStream in) throws IOException {
		long length = VByte.decode(in);
		if (length < 0) {
			throw new IOException("Invalid node length: " + length);
		}
		byte[] bytes = in.readNBytes((int) length);
		if (bytes.length != length) {
			throw new IOException("Unexpected EOF while reading node range");
		}
		long index = VByte.decode(in);
		return new IndexedNode(new CompactString(bytes), index);
	}

	private static void writeNode(OutputStream out, IndexedNode node) throws IOException {
		byte[] bytes = node.getNode().getBuffer();
		int length = node.getNode().length();
		VByte.encode(out, length);
		out.write(bytes, 0, length);
		VByte.encode(out, node.getIndex());
	}
}
