package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PipedCopyIteratorBatchReuseTest {
	@Test
	public void batchArrayIsReusedAfterConsumption() throws Exception {
		PipedCopyIterator<String> pipe = new PipedCopyIterator<>();

		Method borrowArray = PipedCopyIterator.class.getMethod("borrowArray");
		Method addBatch = PipedCopyIterator.class.getMethod("addBatch", Object[].class, int.class);
		Method takeBatch = PipedCopyIterator.class.getMethod("takeBatch");

		AtomicReference<Object[]> firstBatchArray = new AtomicReference<>();
		AtomicReference<Object[]> secondBorrowedArray = new AtomicReference<>();

		CountDownLatch firstBatchPublished = new CountDownLatch(1);
		CountDownLatch firstBatchConsumed = new CountDownLatch(1);

		Thread producer = new Thread(() -> {
			try {
				Object[] array1 = invokeBorrowArray(borrowArray, pipe);
				firstBatchArray.set(array1);
				array1[0] = "a";
				addBatch.invoke(pipe, array1, 1);
				firstBatchPublished.countDown();

				Assert.assertTrue("consumer did not consume in time", firstBatchConsumed.await(5, TimeUnit.SECONDS));

				Object[] array2 = invokeBorrowArray(borrowArray, pipe);
				secondBorrowedArray.set(array2);
				array2[0] = "b";
				addBatch.invoke(pipe, array2, 1);
				pipe.closePipe();
			} catch (Throwable t) {
				try {
					pipe.closePipe(t);
				} catch (Throwable ignored) {
					// ignore
				}
				throw new RuntimeException(t);
			}
		}, "PipedCopyIteratorBatchReuseTest-producer");

		producer.start();

		Assert.assertTrue("producer did not publish in time", firstBatchPublished.await(5, TimeUnit.SECONDS));

		Object batch1 = takeBatch.invoke(pipe);
		Assert.assertNotNull(batch1);
		Method batchArray = batch1.getClass().getMethod("array");
		Method batchSize = batch1.getClass().getMethod("size");
		Object[] readArray1 = (Object[]) batchArray.invoke(batch1);
		int readSize1 = (int) batchSize.invoke(batch1);
		Assert.assertEquals(1, readSize1);
		Assert.assertSame(firstBatchArray.get(), readArray1);
		Assert.assertEquals("a", readArray1[0]);
		((AutoCloseable) batch1).close();
		firstBatchConsumed.countDown();

		Object batch2 = takeBatch.invoke(pipe);
		Assert.assertNotNull(batch2);
		Object[] readArray2 = (Object[]) batchArray.invoke(batch2);
		int readSize2 = (int) batchSize.invoke(batch2);
		Assert.assertEquals(1, readSize2);
		Assert.assertSame(firstBatchArray.get(), readArray2);
		Assert.assertSame(firstBatchArray.get(), secondBorrowedArray.get());
		Assert.assertEquals("b", readArray2[0]);
		((AutoCloseable) batch2).close();

		producer.join(TimeUnit.SECONDS.toMillis(5));
		Assert.assertFalse("producer thread did not terminate", producer.isAlive());

		Object endBatch = takeBatch.invoke(pipe);
		Assert.assertNull(endBatch);
	}

	private static Object[] invokeBorrowArray(Method borrowArray, PipedCopyIterator<?> pipe) {
		try {
			return (Object[]) borrowArray.invoke(pipe);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}
}
