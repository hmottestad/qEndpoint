package com.the_qa_company.qendpoint.core.iterator.utils;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class PipedCopyIteratorClosePipeTest {
	@Test
	public void closePipeWithExceptionDoesNotDrainFromProducer() throws Exception {
		PipedCopyIterator<Integer> pipe = new PipedCopyIterator<>(1);
		pipe.addElement(1);

		Object queue = readField(pipe, "activeReadQueue");
		RingBuffer<?> ringBuffer = (RingBuffer<?>) readField(queue, "ringBuffer");
		Sequence consumerSequence = (Sequence) readField(queue, "consumerSequence");

		assertEquals("Sanity: batch published", 0L, ringBuffer.getCursor());
		assertEquals("Sanity: nothing consumed yet", Sequencer.INITIAL_CURSOR_VALUE, consumerSequence.get());

		pipe.closePipe(new RuntimeException("boom"));

		assertEquals("End marker published", 1L, ringBuffer.getCursor());
		assertEquals("Producer must not drain consumer sequence", Sequencer.INITIAL_CURSOR_VALUE,
				consumerSequence.get());
	}

	private static Object readField(Object instance, String fieldName) throws Exception {
		Field field = instance.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.get(instance);
	}
}
