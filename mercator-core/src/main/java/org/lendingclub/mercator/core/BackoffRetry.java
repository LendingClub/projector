/**
 * Copyright 2017-2018 LendingClub, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lendingclub.mercator.core;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackoffRetry {
	private static final Random rand = new Random();
	private static final Logger logger = LoggerFactory.getLogger(BackoffRetry.class);
	private long initialDelay;
	private long timeout;
	private int maxTries;
	private boolean jitter;

	public BackoffRetry() {
		withTimeout(1, TimeUnit.MINUTES).withInitialDelay(1, TimeUnit.SECONDS);
	}

	public BackoffRetry withTimeout(long t, TimeUnit u) {
		this.timeout = u.toMillis(t);
		return this;
	}

	public BackoffRetry withInitialDelay(long t, TimeUnit u) {
		this.initialDelay = u.toMillis(t);
		return this;
	}

	public BackoffRetry withMaxTries(int maxTries) {
		this.maxTries = maxTries;
		return this;
	}

	public BackoffRetry withJitter(boolean jitter) {
		this.jitter = jitter;
		return this;
	}

	public long getInitialDelay() {
		return initialDelay;
	}

	public long getTimeout() {
		return timeout;
	}

	public int getMaxTries() {
		return maxTries;
	}

	public boolean execute(Supplier<Boolean> action) {
		long t0 = System.currentTimeMillis();
		long delay = initialDelay;
		int n = 0;
		while (true) {
			if (action.get()) {
				return true;
			}
			try {
				long t = jitter ? rand.nextInt((int) delay) : delay;
				logger.info("delaying " + t + " millis before attempt # " + (n + 2));
				sleep(t);
			} catch (InterruptedException e) {
				return false;
			}
			if (maxTries > 0 && (++n == maxTries)) {
				return false;
			}
			long t1 = System.currentTimeMillis();
			if (t1 - t0 >= timeout) {
				return false;
			}
			delay = 5 * delay / 4;
		}
	}

	protected void sleep(long delay) throws InterruptedException {
		Thread.sleep(delay);
	}

}
