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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ScannerContext {

	static Logger logger = LoggerFactory.getLogger(ScannerContext.class);

	Map<String, Object> map = Maps.newConcurrentMap();

	AtomicLong entityScanCount = new AtomicLong();
	long t0 = System.currentTimeMillis();
	private static ThreadLocal<ScannerContext> threadLocalContext = new ThreadLocal<>();
	private List<CleanupTask> cleanupQueue = Lists.newArrayList();

	List<Exception> exceptions = Lists.newArrayList();

	public static interface CleanupTask {
		public void cleanup(ScannerContext context);
	}

	public boolean hasExceptions() {
		return !exceptions.isEmpty();
	}

	public String getName() {
		Object obj = map.get("name");
		if (obj == null) {
			return null;
		} else {
			return obj.toString();
		}
	}

	public Map<String, Object> getMap() {
		return map;
	}

	public ScannerContext withAttribute(String key, Object val) {
		map.put(key, val);
		return this;
	}

	public ScannerContext addCleanupTask(CleanupTask task) {
		this.cleanupQueue.add(task);
		return this;
	}

	public ScannerContext withCleanupTask(CleanupTask task) {
		return addCleanupTask(task);
	}

	public static Optional<ScannerContext> getScannerContext() {
		return Optional.ofNullable(threadLocalContext.get());

	}

	public long getEntityCount() {
		return entityScanCount.get();
	}

	public long incrementEntityCount() {

		return this.entityScanCount.incrementAndGet();
	}

	public List<Exception> getExceptions() {
		return exceptions;
	}

	public ScannerContext markException(Exception e) {
		this.exceptions.add(e);
		return this;
	}

	public ScannerContext withName(String name) {
		getMap().put("name", name);
		return this;
	}

	public void exec(Invokable x) {
		ScannerContext oldContext = threadLocalContext.get();
		ScannerContext newContext = this;
		try {

			threadLocalContext.set(newContext);
			logger.info("start  {}", newContext);

			x.invoke(newContext);
		} finally {
			newContext.cleanup();
			threadLocalContext.set(oldContext);
			newContext.logMetrics("stop");
		
		}
	}

	private void cleanup() {
		logger.debug("cleanup");
		cleanupQueue.forEach(c -> {
			try {
				logger.info("invoking cleanup task: {}", c);
				c.cleanup(this);
			} catch (Exception e) {
				logger.warn("problem during cleanup: " + e);
			}
		});
	}

	public static interface Invokable {

		public void invoke(ScannerContext ctx);

	}

	private void logMetrics(String msg) {
		long tx = System.currentTimeMillis() - t0;
		if (tx == 0) {
			tx = 1;
		}
		double scansPerSecond = (getEntityCount() / (double) tx) * 1000;
		ToStringHelper h = toStringHelper();
		h.add("count", getEntityCount()+" objs");
		h.add("duration", (System.currentTimeMillis() - t0)+" ms");
		h.add("rate", Math.round(scansPerSecond)+ " objs/sec");
		
		logger.info("{} {}",msg,h.toString());
		

	}

	protected ToStringHelper toStringHelper() {
		return MoreObjects.toStringHelper(this).add("name", map.get("name"));

	}

	public String toString() {
		return toStringHelper().toString();
	}
}
