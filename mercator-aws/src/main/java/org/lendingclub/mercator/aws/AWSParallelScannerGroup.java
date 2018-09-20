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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.Scanner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class AWSParallelScannerGroup extends AWSScannerGroup {
	private static class ScannerRunnable implements Runnable {
		private Scanner scanner;

		ScannerRunnable(Scanner scanner) {
			this.scanner = scanner;
		}

		@Override
		public void run() {
			scanner.scan();
		}

	}

	private static final ExecutorService pool;
	static {
		int nThreads = Math.max(3, Runtime.getRuntime().availableProcessors() / 2);
		ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("aws-scanner-%d")
				.build();
		pool = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(), threadFactory);
	}
	private List<Future<?>> tasks = Collections.synchronizedList(new ArrayList<>());

	public static ExecutorService getExecutorService() {
		return pool;
	}

	public AWSParallelScannerGroup(AWSScannerBuilder builder) {
		super(builder);
	}

	@Override
	protected void doScan() {
		if (!builder.getInitialScannerGroup().compareAndSet(null, this)) {
			tasks = builder.getInitialScannerGroup().get().tasks;
		}
		for (Scanner scanner : getScanners()) {
			Future<?> task = pool.submit(new ScannerRunnable(scanner));
			tasks.add(task);
		}
		if (builder.getInitialScannerGroup().get() == this) {
			try {
				waitForCompletion();
			} finally {
				tasks.clear();
			}
		}
	}

	private void waitForCompletion() {
		List<Throwable> failures = new ArrayList<>();
		Set<Future<?>> completed = new HashSet<>();
		while (true) {
			List<Future<?>> pollTasks = new ArrayList<>();
			tasks.forEach(t -> {
				if (completed.add(t)) {
					pollTasks.add(t);
				}
			});
			if (pollTasks.isEmpty()) {
				break;
			}
			logger.info("waiting for " + pollTasks.size() + " parallel scanner groups to complete");
			for (Future<?> task : pollTasks) {
				try {
					task.get();
				} catch (InterruptedException e) {
					throw new MercatorException("scanning interrupted", e);
				} catch (ExecutionException ee) {
					failures.add(ee.getCause());
				}
			}
		}
		logger.info(completed.size() + " parallel scanner groups completed with " + failures.size() + " failures");
		if (!failures.isEmpty()) {
			MercatorException e = new MercatorException("scanning problems", null, true, false);
			failures.forEach(f -> e.addSuppressed(f));
			throw e;
		}
	}

}
