package com.the_qa_company.qendpoint.core.util.listener;

import static org.junit.Assert.assertFalse;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class MultiThreadListenerConsoleTest {
	@Test
	public void notifyProgressDoesNotFormatOnCallingThread() {
		Thread callingThread = Thread.currentThread();
		AtomicBoolean progressBarCalledOnCallerThread = new AtomicBoolean(false);

		MultiThreadListenerConsole console = new MultiThreadListenerConsole(false) {
			@Override
			public String progressBar(float level) {
				if (Thread.currentThread() == callingThread) {
					progressBarCalledOnCallerThread.set(true);
				}
				return "";
			}
		};

		interruptMultiThreadListenerConsoleThreads();

		console.notifyProgress("test-thread", 50f, "test");

		assertFalse("notifyProgress must not format on calling thread", progressBarCalledOnCallerThread.get());
	}

	private static void interruptMultiThreadListenerConsoleThreads() {
		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			if ("MultiThreadListenerConsole".equals(thread.getName())) {
				thread.interrupt();
			}
		}
	}
}
