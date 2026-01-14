package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback.RDFCallback;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RDFParserRIOTThreadSafetyTest {

	@Test
	public void parallelParseDoesNotInvokeCallbackConcurrently() throws Exception {
		String payload = buildNTriplesWithBNodes(2000);
		InputStream input = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

		ConcurrentDetectingCallback callback = new ConcurrentDetectingCallback();
		AtomicReference<Throwable> failure = new AtomicReference<>();

		Thread parserThread = new Thread(() -> {
			try {
				new RDFParserRIOT().doParse(input, "http://base/", RDFNotation.NTRIPLES, true, callback, true);
			} catch (ParserException e) {
				failure.set(e);
			} catch (Throwable t) {
				failure.set(t);
			}
		});
		parserThread.setName("riot-parallel-test");
		parserThread.start();

		assertTrue("Parser never invoked callback", callback.awaitFirstCall(2, TimeUnit.SECONDS));
		callback.releaseFirst();

		parserThread.join(TimeUnit.SECONDS.toMillis(10));
		if (parserThread.isAlive()) {
			parserThread.interrupt();
			throw new AssertionError("Parser thread did not finish in time");
		}

		Throwable thrown = failure.get();
		if (thrown != null) {
			throw new AssertionError("Parser threw exception", thrown);
		}

		assertFalse("Parallel parse invoked callback concurrently", callback.sawConcurrent());
	}

	private static String buildNTriplesWithBNodes(int pairs) {
		StringBuilder sb = new StringBuilder(pairs * 80);
		for (int i = 0; i < pairs; i++) {
			sb.append("<http://ex/s").append(i).append("> <http://ex/p> \"o").append(i).append("\" .\n");
			sb.append("_:b").append(i).append(" <http://ex/p> \"o").append(i).append("\" .\n");
		}
		return sb.toString();
	}

	private static final class ConcurrentDetectingCallback implements RDFCallback {
		private final AtomicInteger inFlight = new AtomicInteger(0);
		private final AtomicBoolean concurrent = new AtomicBoolean(false);
		private final AtomicBoolean first = new AtomicBoolean(true);
		private final CountDownLatch firstEntered = new CountDownLatch(1);
		private final CountDownLatch releaseFirst = new CountDownLatch(1);

		@Override
		public void processTriple(TripleString triple, long pos) {
			int active = inFlight.incrementAndGet();
			if (active > 1) {
				concurrent.set(true);
			}
			if (first.compareAndSet(true, false)) {
				firstEntered.countDown();
				try {
					releaseFirst.await(2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			inFlight.decrementAndGet();
		}

		boolean awaitFirstCall(long timeout, TimeUnit unit) throws InterruptedException {
			return firstEntered.await(timeout, unit);
		}

		void releaseFirst() {
			releaseFirst.countDown();
		}

		boolean sawConcurrent() {
			return concurrent.get();
		}
	}
}
