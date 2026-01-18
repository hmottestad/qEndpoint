package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class TripleOrderConvertTest {
	@Test
	public void getSwapLambda_matches_swapComponentOrder_forAllPairs() throws Exception {
		Method method = TripleOrderConvert.class.getMethod("getSwapLambda", TripleComponentOrder.class,
				TripleComponentOrder.class);

		for (TripleComponentOrder from : TripleComponentOrder.values()) {
			if (from == TripleComponentOrder.Unknown) {
				continue;
			}
			for (TripleComponentOrder to : TripleComponentOrder.values()) {
				if (to == TripleComponentOrder.Unknown) {
					continue;
				}

				@SuppressWarnings("unchecked")
				Consumer<TripleID> swap = (Consumer<TripleID>) method.invoke(null, from, to);

				TripleID base = new TripleID(1, 2, 3);
				TripleOrderConvert.swapComponentOrder(base, TripleComponentOrder.SPO, from);

				TripleID expected = base.clone();
				TripleID actual = base.clone();

				TripleOrderConvert.swapComponentOrder(expected, from, to);
				swap.accept(actual);

				assertEquals("from=" + from + ", to=" + to, expected, actual);
			}
		}
	}
}
