package com.the_qa_company.qendpoint.core.iterator.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PipedCopyIteratorIdleStrategyTest {
	@Test
	public void idleStrategyIsUsedWhenPolling() throws Exception {
		Class<?> idleStrategyType = Class
				.forName("com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator$IdleStrategy");
		AtomicInteger idleCalls = new AtomicInteger();
		CountDownLatch idleInvoked = new CountDownLatch(1);

		Object idleStrategy = Proxy.newProxyInstance(idleStrategyType.getClassLoader(),
				new Class<?>[] { idleStrategyType }, (proxy, method, args) -> {
					if ("idle".equals(method.getName())) {
						idleCalls.incrementAndGet();
						idleInvoked.countDown();
						return ((Integer) args[0]) + 1;
					}
					throw new UnsupportedOperationException(method.toString());
				});

		Constructor<PipedCopyIterator> ctor = PipedCopyIterator.class.getDeclaredConstructor(int.class,
				idleStrategyType);
		ctor.setAccessible(true);
		PipedCopyIterator<String> pipe = ctor.newInstance(1, idleStrategy);

		Thread consumer = new Thread(() -> {
			try {
				pipe.hasNext();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, "PipedCopyIteratorIdleStrategyTest-consumer");

		consumer.start();

		Assert.assertTrue("idle strategy was not invoked", idleInvoked.await(5, TimeUnit.SECONDS));
		Assert.assertTrue("idle strategy was not invoked enough times", idleCalls.get() > 0);

		pipe.closePipe();
		consumer.join(TimeUnit.SECONDS.toMillis(5));
		Assert.assertFalse("consumer thread did not terminate", consumer.isAlive());
	}
}
