package com.the_qa_company.qendpoint.core.util.listener;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MultiThreadListenerConsoleTest {
	@Test
	public void rendersFromBackgroundThread() throws Exception {
		PrintStream originalOut = System.out;
		LatchOutputStream out = new LatchOutputStream("hello");
		System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
		try {
			MultiThreadListenerConsole console = new MultiThreadListenerConsole(false, true);
			console.notifyProgress("worker", 12.3f, "hello");
			Assert.assertFalse("Expected delayed render", out.await(20, TimeUnit.MILLISECONDS));
			Assert.assertTrue("Expected render within 1s", out.await(1, TimeUnit.SECONDS));
		} finally {
			System.setOut(originalOut);
		}
	}

	private static final class LatchOutputStream extends OutputStream {
		private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		private final CountDownLatch latch = new CountDownLatch(1);
		private final byte[] needle;

		private LatchOutputStream(String needle) {
			this.needle = needle.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public synchronized void write(int b) {
			buffer.write(b);
			check();
		}

		@Override
		public synchronized void write(byte[] b, int off, int len) {
			buffer.write(b, off, len);
			check();
		}

		private void check() {
			if (latch.getCount() == 0) {
				return;
			}
			byte[] data = buffer.toByteArray();
			if (indexOf(data, needle) >= 0) {
				latch.countDown();
			}
		}

		private int indexOf(byte[] haystack, byte[] needle) {
			if (needle.length == 0 || haystack.length < needle.length) {
				return -1;
			}
			outer:
			for (int i = 0; i <= haystack.length - needle.length; i++) {
				for (int j = 0; j < needle.length; j++) {
					if (haystack[i + j] != needle[j]) {
						continue outer;
					}
				}
				return i;
			}
			return -1;
		}

		private boolean await(long timeout, TimeUnit unit) throws InterruptedException {
			return latch.await(timeout, unit);
		}
	}
}
