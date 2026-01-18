package com.the_qa_company.qendpoint.core.triples;

import org.junit.Assert;
import org.junit.Test;

public class TripleIDMatchTest {

	@Test
	public void matchTreatsZeroAsWildcard() {
		TripleID triple = new TripleID(1, 2, 3, 4);
		TripleID pattern = new TripleID(1, 0, 3, 0);

		Assert.assertTrue(triple.match(pattern));
	}

	@Test
	public void matchRejectsNonMatchingComponent() {
		TripleID triple = new TripleID(1, 2, 3, 4);
		TripleID pattern = new TripleID(1, 5, 3, 4);

		Assert.assertFalse(triple.match(pattern));
	}

	@Test
	public void matchRespectsGraphWhenPatternNonZero() {
		TripleID triple = new TripleID(1, 2, 3);
		TripleID pattern = new TripleID(1, 2, 3, 1);

		Assert.assertFalse(triple.match(pattern));
	}
}
