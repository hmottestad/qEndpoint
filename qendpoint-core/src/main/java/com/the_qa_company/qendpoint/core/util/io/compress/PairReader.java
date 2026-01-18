package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.CRCStopBitInputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reader for file wrote with {@link PairWriter}
 *
 * @author Antoine Willerval
 */
public class PairReader implements ExceptionIterator<Pair, IOException>, Closeable,
		RangeAwareMergeExceptionIterator.RangedExceptionIterator<Pair, IOException> {
	private final CRCInputStream stream;
	private final Pair next = new Pair();
	private boolean read = false, end = false;
	private final long size;
	private long index;
	private final RangeAwareMergeExceptionIterator.KeyRange<Pair> range;

	public PairReader(InputStream stream) throws IOException {
		this(stream, null);
	}

	public PairReader(CloseSuppressPath path, int bufferSize) throws IOException {
		this(path.openInputStream(bufferSize), PairRange.readRangeIfExists(path));
	}

	private PairReader(InputStream stream, RangeAwareMergeExceptionIterator.KeyRange<Pair> range) throws IOException {
		this.stream = new CRCStopBitInputStream(stream, new CRC32());
		size = VByte.decode(this.stream);
		this.range = range;
	}

	public long getSize() {
		return size;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (read) {
			return true;
		}

		// the reader is empty, null end triple
		if (end) {
			return false;
		}

		if (index == size) {
			end = true;
			if (!stream.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading PreMapped pairs.");
			}
			return false;
		}

		index++;

		long p = VByte.decodeSigned(stream);
		long v = VByte.decodeSigned(stream);
		long pred = VByte.decodeSigned(stream);

		return !setAllOrEnd(p, v, pred);
	}

	private boolean setAllOrEnd(long p, long v, long pred) {
		if (end) {
			// already completed
			return true;
		}
		// map the triples to the end id, compute the shared with the end shared
		// size
		next.increaseAll(p, v, pred);
		read = true;
		return false;
	}

	@Override
	public Pair next() throws IOException {
		if (!hasNext()) {
			return null;
		}
		read = false;
		return next;
	}

	@Override
	public RangeAwareMergeExceptionIterator.KeyRange<Pair> keyRange() {
		return range;
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}
}
