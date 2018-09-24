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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.lendingclub.mercator.core.ScannerContext.CleanupTask;

public class ScannerContextTest {

	@Test
	public void testNoContext() {

		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();
	}

	@Test
	public void testContext() {

		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();

		new ScannerContext().withName("test").exec(ctx -> {
			Assertions.assertThat(ctx.getEntityCount()).isEqualTo(0);
			Assertions.assertThat(ScannerContext.getScannerContext().get()).isSameAs(ctx);
		});

		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();

	}

	@Test
	public void testExceptionThrownFromContext() {
		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();


		try {
			new ScannerContext().exec(ctx -> {
				throw new RuntimeException("simulated failure");
			});
		} catch (RuntimeException e) {
			Assertions.assertThat(e).hasMessageContaining("simulated failure");
		}

		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();

	}

	@Test
	public void testNestedContext() {
		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();

		new ScannerContext().withName("test").exec(ctx -> {
			Assertions.assertThat(ctx.getEntityCount()).isEqualTo(0);
			Assertions.assertThat(ScannerContext.getScannerContext().get()).isSameAs(ctx);

			new ScannerContext().withName("inner").exec(innerCtx -> {
				Assertions.assertThat(ctx.getEntityCount()).isEqualTo(0);
				Assertions.assertThat(ScannerContext.getScannerContext().get()).isSameAs(innerCtx);

			});

			Assertions.assertThat(ctx.getEntityCount()).isEqualTo(0);
			Assertions.assertThat(ScannerContext.getScannerContext().get()).isSameAs(ctx);
		});

		Assertions.assertThat(ScannerContext.getScannerContext().isPresent()).isFalse();

	}

	@Test
	public void testCleanup() throws InterruptedException {

		CountDownLatch latch = new CountDownLatch(1);
		CleanupTask task = new CleanupTask() {

			@Override
			public void cleanup(ScannerContext context) {
				latch.countDown();

			}
		};
		new ScannerContext().withName("test").withCleanupTask(task).exec(ctx -> {
			Assertions.assertThat(ctx.getMap().get("name")).isEqualTo("test");
		});

		Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

	}
}
