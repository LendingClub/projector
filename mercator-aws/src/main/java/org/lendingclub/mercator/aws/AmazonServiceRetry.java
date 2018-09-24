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
package org.lendingclub.mercator.aws;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.lendingclub.mercator.core.BackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.retry.RetryUtils;

public class AmazonServiceRetry {
	private static final Logger logger = LoggerFactory.getLogger(AmazonServiceRetry.class);

	public interface ShouldRetry extends Function<AmazonServiceException, Boolean> {
	}

	public static final ShouldRetry THROTTLING = (e) -> RetryUtils.isThrottlingException((SdkBaseException) e)
			|| "LimitExceededException".equals(e.getErrorCode()); // RetryUtils is unaware of Kinesis?

	public static <T> T execute(Supplier<T> action) {

		return new AmazonServiceRetry().call(action);

	}

	public static <T> T execute(BackoffRetry retry, Supplier<T> action) {
		return new AmazonServiceRetry().withRetry(retry).call(action);
	}

	private BackoffRetry retry;
	private ShouldRetry shouldRetry = THROTTLING;

	public AmazonServiceRetry() {
	}

	public <T> T call(Supplier<T> action) {
		init();
		Object[] holder = new Object[1];
		boolean[] success = new boolean[1];
		retry.execute(() -> {
			try {
				holder[0] = action.get();
				success[0] = true;
				return true;
			} catch (AmazonServiceException e) {
				holder[0] = e;
				/*
				 * Each amazon service has its own way of indicating throttling, but fortunately
				 * they've got an API to tell you.
				 */
				if (shouldRetry.apply(e)) {
					logger.warn(e.getMessage());
					return false;
				}
				return true;
			}
		});
		if (success[0]) {
			@SuppressWarnings("unchecked")
			T result = (T) holder[0];
			return result;
		}
		if (holder[0] != null) {
			throw (AmazonServiceException) holder[0];
		}
		throw new AmazonServiceException("operation failed after retries");
	}

	private void init() {
		if (retry == null) {
			// default - try 3 more times taking (30 + 60 + 120) / 2 = 105
			// seconds (average max)
			retry = new BackoffRetry().withMaxTries(4).withInitialDelay(30, TimeUnit.SECONDS).withJitter(true);
		}
	}

	public AmazonServiceRetry withRetry(BackoffRetry retry) {
		this.retry = retry;
		return this;
	}

	public AmazonServiceRetry withShouldRetry(ShouldRetry shouldRetry) {
		this.shouldRetry = shouldRetry;
		return this;
	}

	public AmazonServiceRetry withChainedShouldRetry(ShouldRetry shouldRetry) {
		final ShouldRetry base = this.shouldRetry;
		this.shouldRetry = (e) -> base.apply(e) || shouldRetry.apply(e);
		return this;
	}

	public AmazonServiceRetry withRetryOnErrorCode(String errorCode) {
		return withChainedShouldRetry(e -> errorCode.equals(e.getErrorCode()));
	}

}
