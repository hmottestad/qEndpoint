package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RangeAwareMergeExceptionIteratorTest {

	@Test
	public void mergeViaReflectionUsesRangesWhenPresent() throws Exception {
		Class<?> clazz = Class
				.forName("com.the_qa_company.qendpoint.core.iterator.utils.RangeAwareMergeExceptionIterator");
		Method ranged = clazz.getMethod("ranged", ExceptionIterator.class, Object.class, Object.class);
		Method merge = clazz.getMethod("merge", List.class, Comparator.class);

		ExceptionIterator<Integer, RuntimeException> it1 = ExceptionIterator.of(List.of(1, 3).iterator());
		ExceptionIterator<Integer, RuntimeException> it2 = ExceptionIterator.of(List.of(2, 4).iterator());

		Object r1 = ranged.invoke(null, it1, 1, 3);
		Object r2 = ranged.invoke(null, it2, 2, 4);

		Comparator<Integer> comparator = Integer::compareTo;
		@SuppressWarnings("unchecked")
		ExceptionIterator<Integer, RuntimeException> merged = (ExceptionIterator<Integer, RuntimeException>) merge
				.invoke(null, new Object[] { List.of(r1, r2), comparator });

		List<Integer> out = new ArrayList<>();
		while (merged.hasNext()) {
			out.add(merged.next());
		}

		assertEquals(Arrays.asList(1, 2, 3, 4), out);
	}
}
