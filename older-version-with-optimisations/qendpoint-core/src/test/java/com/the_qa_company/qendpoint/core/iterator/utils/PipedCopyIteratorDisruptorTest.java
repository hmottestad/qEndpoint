package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class PipedCopyIteratorDisruptorTest {
	@Test
	public void usesDisruptorQueues() throws Exception {
		PipedCopyIterator<String> pipe = new PipedCopyIterator<>();

		Object readQueue = readField(pipe, "activeReadQueue");
		Object writeQueue = readField(pipe, "activeWriteQueue");
		Object recycleQueue = readField(pipe, "recycledArrays");

		Assert.assertNotNull(readQueue);
		Assert.assertNotNull(writeQueue);
		Assert.assertNotNull(recycleQueue);

		Assert.assertEquals(readQueue.getClass(), writeQueue.getClass());
		Assert.assertEquals(readQueue.getClass(), recycleQueue.getClass());

		Field ringBufferField = readQueue.getClass().getDeclaredField("ringBuffer");
		ringBufferField.setAccessible(true);
		Object ringBuffer = ringBufferField.get(readQueue);
		Assert.assertNotNull(ringBuffer);
		Assert.assertEquals("com.lmax.disruptor.RingBuffer", ringBuffer.getClass().getName());
	}

	private static Object readField(Object instance, String name) throws Exception {
		Field field = instance.getClass().getDeclaredField(name);
		field.setAccessible(true);
		return field.get(instance);
	}
}
