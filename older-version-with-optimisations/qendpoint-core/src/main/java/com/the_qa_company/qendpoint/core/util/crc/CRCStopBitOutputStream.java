package com.the_qa_company.qendpoint.core.util.crc;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Buffered {@link CRCOutputStream} with a fast stop-bit (VByte-style) long
 * writer.
 * <p>
 * Properties:
 * <ul>
 * <li>Normal writes update CRC.</li>
 * <li>{@link #writeCRC()} writes digest bytes without updating CRC and
 * preserves correct ordering even when data is buffered.</li>
 * </ul>
 * </p>
 */
public final class CRCStopBitOutputStream extends CRCOutputStream implements VByte.FastOutput {
	private static final int MIN_VARINT_BYTES = 9;

	private final byte[] buf;
	private int pos;

	private final OutputStream crcBypass = new OutputStream() {
		@Override
		public void write(int b) throws IOException {
			writeRawByte(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			writeRawBytes(b, off, len);
		}
	};

	public CRCStopBitOutputStream(OutputStream out, CRC crc) {
		this(out, crc, 1 << 15);
	}

	public CRCStopBitOutputStream(OutputStream out, CRC crc, int bufferSize) {
		super(out, crc);
		if (bufferSize < MIN_VARINT_BYTES) {
			throw new IllegalArgumentException("bufferSize must be >= " + MIN_VARINT_BYTES);
		}
		this.buf = new byte[bufferSize];
	}

	@Override
	public void writeCRC() throws IOException {
		crc.writeCRC(crcBypass);
	}

	@Override
	public void write(int b) throws IOException {
		if (pos == buf.length) {
			flushBuffer();
		}
		buf[pos++] = (byte) b;
		crc.update((byte) b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return;
		}

		crc.update(b, off, len);

		if (len >= buf.length) {
			flushBuffer();
			out.write(b, off, len);
			return;
		}

		if (len > buf.length - pos) {
			flushBuffer();
		}
		System.arraycopy(b, off, buf, pos, len);
		pos += len;
	}

	@Override
	public void flush() throws IOException {
		flushBuffer();
		out.flush();
	}

	@Override
	public void writeVByteLong(long value) throws IOException {
		if (value < 0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}

		final int bits = 64 - Long.numberOfLeadingZeros(value | 1L);
		final int needed = (bits + 6) / 7;

		if (buf.length - pos < needed) {
			flushBuffer();
		}

		final int start = pos;
		long v = value;

		while (true) {
			final int payload = (int) v & 0x7F;
			v >>>= 7;
			if (v == 0) {
				buf[pos++] = (byte) (payload | 0x80);
				break;
			} else {
				buf[pos++] = (byte) payload;
			}
		}

		updateCrcFromArray(buf, start, pos - start);
	}

	private void flushBuffer() throws IOException {
		if (pos > 0) {
			out.write(buf, 0, pos);
			pos = 0;
		}
	}

	private void updateCrcFromArray(byte[] a, int off, int len) {
		if (len <= 0) {
			return;
		}
		if (len == 1) {
			crc.update(a[off]);
		} else {
			crc.update(a, off, len);
		}
	}

	private void writeRawByte(int b) throws IOException {
		if (pos == buf.length) {
			flushBuffer();
		}
		buf[pos++] = (byte) b;
	}

	private void writeRawBytes(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("b");
		}
		if ((off | len) < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}
		if (len == 0) {
			return;
		}

		if (len >= buf.length) {
			flushBuffer();
			out.write(b, off, len);
			return;
		}

		if (len > buf.length - pos) {
			flushBuffer();
		}
		System.arraycopy(b, off, buf, pos, len);
		pos += len;
	}
}
