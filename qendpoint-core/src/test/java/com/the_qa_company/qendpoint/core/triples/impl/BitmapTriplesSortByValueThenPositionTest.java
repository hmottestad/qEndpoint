package com.the_qa_company.qendpoint.core.triples.impl;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class BitmapTriplesSortByValueThenPositionTest {
	private static final class Pair {
		private final long value;
		private final long position;

		private Pair(long value, long position) {
			this.value = value;
			this.position = position;
		}
	}

	@Test
	public void sortByValueThenPositionOrdersPairs() throws Exception {
		long[] values = { 5L, 2L, 2L, 1L, 5L, 3L };
		long[] positions = { 9L, 4L, 1L, 7L, 3L, 8L };

		Pair[] expected = new Pair[values.length];
		for (int i = 0; i < values.length; i++) {
			expected[i] = new Pair(values[i], positions[i]);
		}
		Arrays.sort(expected,
				Comparator.comparingLong((Pair pair) -> pair.value).thenComparingLong(pair -> pair.position));

		Method method = BitmapTriples.class.getDeclaredMethod("sortByValueThenPosition", long[].class, long[].class,
				int.class, int.class);
		method.setAccessible(true);
		method.invoke(null, values, positions, 0, values.length);

		for (int i = 0; i < values.length; i++) {
			assertEquals(expected[i].value, values[i]);
			assertEquals(expected[i].position, positions[i]);
		}
	}
}
