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

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class RateLimiterTest {

	public static class FakeScannerBuilder extends ScannerBuilder<FakeScanner> {

		@Override
		public FakeScanner build() {
			return new FakeScanner(this);
		}

	}
	public static class FakeScanner extends AbstractScanner {

	    Logger logger = LoggerFactory.getLogger(FakeScanner.class);
		public FakeScanner(ScannerBuilder<? extends Scanner> builder) {
			super(builder);
			
		}

		@Override
		public void scan() {
			logger.info("scan() called");
			for (int i=0; i<10; i++) {
				logger.info("doing something...");
				rateLimit();
				
			}
			logger.info("scan() complete");

		}

	}

	@Test
	public void testDefault() {
		Stopwatch sw = Stopwatch.createStarted();
		Projector p = new Projector.Builder().withUrl("bolt://localhost:12345").build();
		p.createBuilder(FakeScannerBuilder.class).build().scan();
		Assertions.assertThat(sw.elapsed(TimeUnit.SECONDS)).isLessThan(3);	
	}
	
	@Test
	public void testZero() {
		Stopwatch sw = Stopwatch.createStarted();
		Projector p = new Projector.Builder().withUrl("bolt://localhost:12345").build();
		p.createBuilder(FakeScannerBuilder.class).withRateLimitPerSecond(0).build().scan();
		Assertions.assertThat(sw.elapsed(TimeUnit.SECONDS)).isLessThan(3);	
	}
	
	@Test
	public void testNegative() {
		Stopwatch sw = Stopwatch.createStarted();
		Projector p = new Projector.Builder().withUrl("bolt://localhost:12345").build();
		p.createBuilder(FakeScannerBuilder.class).withRateLimitPerSecond(-1.0).build().scan();
		Assertions.assertThat(sw.elapsed(TimeUnit.SECONDS)).isLessThan(3);	
	}
	
	@Test
	public void testRateLimit() {
		Stopwatch sw = Stopwatch.createStarted();
		Projector p = new Projector.Builder().withUrl("bolt://localhost:12345").build();
		p.createBuilder(FakeScannerBuilder.class).withRateLimitPerSecond(2.0).build().scan();
		Assertions.assertThat(sw.elapsed(TimeUnit.SECONDS)).isGreaterThan(3);
	}
}
