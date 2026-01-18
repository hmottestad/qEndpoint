package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class CompressFourSectionDictionaryBulkWriteTest {
	@Test
	public void usesBulkNodeConsumerCallsIn1024Batches() throws Exception {
		// Guardrail for the intended API shape: a bulk overload accepting
		// primitive arrays.
		CompressFourSectionDictionary.NodeConsumer.class.getMethod("onSubject", long[].class, long[].class, int.class,
				int.class);
		CompressFourSectionDictionary.NodeConsumer.class.getMethod("onObject", long[].class, long[].class, int.class,
				int.class);

		AtomicInteger subjectBulkCalls = new AtomicInteger();
		AtomicInteger objectBulkCalls = new AtomicInteger();
		AtomicInteger maxBulkLength = new AtomicInteger();

		CompressFourSectionDictionary.NodeConsumer consumer = (CompressFourSectionDictionary.NodeConsumer) Proxy
				.newProxyInstance(getClass().getClassLoader(),
						new Class<?>[] { CompressFourSectionDictionary.NodeConsumer.class }, (proxy, method, args) -> {
							if (method.getName().equals("onSubject") && method.getParameterCount() == 4
									&& method.getParameterTypes()[0] == long[].class) {
								subjectBulkCalls.incrementAndGet();
								int length = (int) args[3];
								maxBulkLength.updateAndGet(prev -> Math.max(prev, length));
								return null;
							}
							if (method.getName().equals("onObject") && method.getParameterCount() == 4
									&& method.getParameterTypes()[0] == long[].class) {
								objectBulkCalls.incrementAndGet();
								int length = (int) args[3];
								maxBulkLength.updateAndGet(prev -> Math.max(prev, length));
								return null;
							}
							// ignore single-element calls and other sections
							// for this test
							return null;
						});

		int elementCount = 2050;
		CharSequence[] subjects = new CharSequence[elementCount];
		CharSequence[] predicates = new CharSequence[] { "p" };
		CharSequence[] objects = new CharSequence[elementCount];
		Arrays.fill(subjects, "aaa");
		Arrays.fill(objects, "bbb");

		CompressionResult result = new TestCompressionResult(subjects, predicates, objects);

		try (CompressFourSectionDictionary dictionary = new CompressFourSectionDictionary(result, consumer,
				(p, m) -> {}, false, false)) {
			drain(dictionary.getSubjects().getSortedEntries());
			drain(dictionary.getPredicates().getSortedEntries());
			drain(dictionary.getObjects().getSortedEntries());
			drain(dictionary.getShared().getSortedEntries());
		}

		Assert.assertTrue("Expected at least one bulk subject write", subjectBulkCalls.get() > 0);
		Assert.assertTrue("Expected at least one bulk object write", objectBulkCalls.get() > 0);
		Assert.assertEquals("Expected bulk batches up to 1024 items", 1024, maxBulkLength.get());
	}

	private static void drain(Iterator<? extends CharSequence> it) {
		while (it.hasNext()) {
			it.next();
		}
	}

	static class TestCompressionResult implements CompressionResult {
		private final CharSequence[] subjects;
		private final CharSequence[] predicates;
		private final CharSequence[] objects;
		private final int tripleCount;
		private int sid;
		private int pid;
		private int oid;

		TestCompressionResult(CharSequence[] subjects, CharSequence[] predicates, CharSequence[] objects) {
			this.subjects = subjects;
			this.predicates = predicates;
			this.objects = objects;
			this.tripleCount = Math.max(subjects.length, Math.max(predicates.length, objects.length));
		}

		@Override
		public long getTripleCount() {
			return tripleCount;
		}

		@Override
		public boolean supportsGraph() {
			return false;
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getSubjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(subjects).iterator(),
					s -> new IndexedNode(ByteString.of(s), sid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getPredicates() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(predicates).iterator(),
					p -> new IndexedNode(ByteString.of(p), pid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getObjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(objects).iterator(),
					o -> new IndexedNode(ByteString.of(o), oid++)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getGraph() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getSubjectsCount() {
			return subjects.length;
		}

		@Override
		public long getPredicatesCount() {
			return predicates.length;
		}

		@Override
		public long getObjectsCount() {
			return objects.length;
		}

		@Override
		public long getGraphCount() {
			return 0;
		}

		@Override
		public long getSharedCount() {
			return 0;
		}

		@Override
		public long getRawSize() {
			return 0;
		}

		@Override
		public void delete() {
		}

		@Override
		public void close() {
		}
	}
}
