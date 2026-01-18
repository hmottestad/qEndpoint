package com.the_qa_company.qendpoint.core.util.io.compress;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCStopBitOutputStream;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class to write a compress node file
 *
 * @author Antoine Willerval
 */
public class CompressNodeWriter implements Closeable {
	static final byte[] MAGIC = new byte[] { 'C', 'N', 'L', '1' };

	private final CRCOutputStream out;
	private final ReplazableString previousStr = new ReplazableString();

	public CompressNodeWriter(OutputStream stream, long size) throws IOException {
		stream.write(MAGIC);
		this.out = new CRCStopBitOutputStream(stream, new CRC8());
		VByte.encode(this.out, size);
		this.out.writeCRC();
		this.out.setCRC(new CRC32());
	}

	public void appendNode(IndexedNode node) throws IOException {
		ByteString str = node.getNode();
		long index = node.getIndex();

		// Find common part.
		int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
		int suffixLen = countNonZeroBytes(str, delta);
		// Write Delta in VByte
		VByte.encode(out, delta);
		VByte.encode(out, suffixLen);
		// Write remaining
		ByteStringUtil.append(out, str, delta);
		VByte.encode(out, index); // index of the node
		previousStr.replace(str);
	}

	private static int countNonZeroBytes(ByteString str, int start) {
		byte[] bytes = str.getBuffer();
		int len = str.length();
		int count = 0;
		for (int i = start; i < len; i++) {
			if (bytes[i] != 0) {
				count++;
			}
		}
		return count;
	}

	public void writeCRC() throws IOException {
		out.writeCRC();
		out.flush();
	}

	@Override
	public void close() throws IOException {
		writeCRC();
		out.close();
	}
}
