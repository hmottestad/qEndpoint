package com.the_qa_company.qendpoint.core.listener;

import org.junit.Test;

public class ProgressListenerLazyTest {
	@Test
	public void ignoreDoesNotRenderTemplates() {
		Object explodingToString = new Object() {
			@Override
			public String toString() {
				throw new AssertionError("toString should not be called");
			}
		};

		ProgressListener.ignore().notifyProgress(0f, "should not render {}", explodingToString);
	}

	@Test
	public void multiThreadIgnoreDoesNotRenderTemplates() {
		Object explodingToString = new Object() {
			@Override
			public String toString() {
				throw new AssertionError("toString should not be called");
			}
		};

		MultiThreadListener.ignore().notifyProgress("thread", 0f, "should not render {}", explodingToString);
	}
}
