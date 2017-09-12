/**
 * Copyright 2017 Lending Club, Inc.
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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;

public abstract class AbstractScanner implements Scanner {

	Logger logger = LoggerFactory.getLogger(getClass());

	ScannerBuilder<? extends Scanner> builder;

	protected RateLimiter rateLimiter = null;

	public AbstractScanner(ScannerBuilder<? extends Scanner> builder) {
		this.builder = builder;

		Preconditions.checkNotNull(builder, "builder cannot be null");
		Preconditions.checkNotNull(builder.getProjector(), "builder.getProjector() cannot be null");

		double rateLimit = -1;
		if (builder.getRateLimitPerSecond().isPresent() == false) {
			// If the rate limit was not explicitly set. That is, if the caller
			// did not request a specific limit or "no limit",
			// set it to the Scanner-specific default or "no-limit"
			rateLimit = getDefaultRateLimitPerSecond().orElse(-1.0d);
		} else {
			// rate limit was explicitly set in the builder
			rateLimit = builder.getRateLimitPerSecond().orElse(-1d);
		}

		if (rateLimit > 0) {
			logger.info("rate limit {} calls/second", rateLimit);
			this.rateLimiter = RateLimiter.create(rateLimit);
		} else {
			logger.info("rate limit {} calls/second", "unlimited");
			this.rateLimiter = null;
		}

	}

	protected ScannerBuilder<? extends Scanner> getBuilder() {
		return this.builder;
	}

	@Override
	public Projector getProjector() {
		return builder.getProjector();
	}

	public NeoRxClient getNeoRxClient() {
		return getProjector().getNeoRxClient();
	}

	public boolean isFailOnError() {
		return builder.isFailOnError();
	}

	public void maybeThrow(Exception e, String message) {
		ScannerContext.getScannerContext().ifPresent(sc -> {
			sc.markException(e);
		});
		if (isFailOnError()) {
			if (e instanceof RuntimeException) {
				throw ((RuntimeException) e);
			} else {
				throw new MercatorException(e);
			}
		} else {
			if (shouldLogStackTrace(e)) {
				logger.warn(message, e);
			}
			else {
				logger.warn(message+" - "+e.toString());
			}
			
		}
	}
	public boolean shouldLogStackTrace(Throwable t) {
		return true;
	}
	public void maybeThrow(Exception e) {
		maybeThrow(e, "scanning problem");

	}

	public SchemaManager getSchemaManager() {
		return new SchemaManager(getProjector().getNeoRxClient());
	}

	@Override
	public void rateLimit() {

		if (rateLimiter != null) {
			Stopwatch sw = Stopwatch.createStarted();
			rateLimiter.acquire();
			sw.stop();
			long ms = sw.elapsed(TimeUnit.MILLISECONDS);
			if (ms > 50) {
				logger.info("rate limiting paused execution for {} ms",ms);
			}

		}

	}

	public java.util.Optional<Double> getDefaultRateLimitPerSecond() {
		return Optional.empty();
	}
}
