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
package org.lendingclub.mercator.docker;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Closer;


public class DockerEventProcessor {

	Logger logger = LoggerFactory.getLogger(DockerEventProcessor.class);

	ScheduledExecutorService watchdogExecutor = null;

	
	DockerRestClient client;
	
	// We use the supplier in case we need to be able to create a new client after intiailization.
	// For instance, if authentication changes. 
	Supplier<DockerRestClient> restClientSupplier = null;

	ObjectMapper mapper = new ObjectMapper();
	AtomicLong lastEventNano = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));
	List<Consumer<JsonNode>> eventConsumers = Lists.newCopyOnWriteArrayList();
	ThreadPoolExecutor threadPoolExecutor;
	com.google.common.base.Supplier<String> swarmClusterIdSupplier = Suppliers
			.memoizeWithExpiration(new SwarmClusterIdSupplier(), 10, TimeUnit.MINUTES);

	
	Supplier<Boolean> dispatchingEnabledSupplier = new Supplier<Boolean>() {

		@Override
		public Boolean get() {
			return true;
		}
	};
	BlockingQueue<Runnable> queue;

	DockerEventProcessor() {

	}

	public void shutdown() {
		this.watchdogExecutor.shutdown();
		this.threadPoolExecutor.shutdown();
	}

	public synchronized DockerRestClient getRestClient() {
		if (client==null) {
			client = restClientSupplier.get();
		}
		return client;
	}
	public void addConsumer(Consumer<JsonNode> consumer) {
		this.eventConsumers.add(consumer);

	}

	class SwarmClusterIdSupplier implements com.google.common.base.Supplier<String> {

		@Override
		public String get() {

			return getRestClient().getInfo().path("Swarm").path("Cluster").path("ID").asText(null);
		}

	}

	public static class Builder {

		DockerEventProcessor p = new DockerEventProcessor();

		public Builder addConsumer(Consumer<JsonNode> consumer) {
			this.p.eventConsumers.add(consumer);
			return this;

		}

		public Builder withDispatchingEnabledSupplier(Supplier<Boolean> b) {
			p.dispatchingEnabledSupplier = b;
			return this;
		}

		public Builder withDockerRestClient(DockerRestClient c) {
			p.restClientSupplier = Suppliers.memoize(new com.google.common.base.Supplier() {
			

			
				public DockerRestClient get() {
					return c;
				}
			});
			return this;
		}

		public DockerEventProcessor build() {

			p.queue = new LinkedBlockingDeque<>(1000);
			p.threadPoolExecutor = new ThreadPoolExecutor(1, 2, 3, TimeUnit.SECONDS, p.queue);

			p.threadPoolExecutor.prestartAllCoreThreads();
			p.watchdogExecutor = Executors.newSingleThreadScheduledExecutor();
			p.watchdogExecutor.scheduleWithFixedDelay(p.new EventContext(), 0, 5, TimeUnit.SECONDS);
			return p;
		}
	}

	class EventContext implements Runnable {

		@Override
		public void run() {

			logger.debug("polling for events from {} ", this);
			Closer closer = Closer.create();
			try {
				long since = lastEventNano.get() + 1;
				long until = System.currentTimeMillis() * 1000000;
				until = Math.max(since, until);
				Reader r = getRestClient().getWebTarget().path("/events")
						.queryParam("since", formatNanoTime(since)).queryParam("until", formatNanoTime(until)).request()
						.get(Reader.class);
				closer.register(r);
				StringBuffer sb = new StringBuffer();
				CharStreams.readLines(r).forEach(it -> {
					if (it.startsWith("{")) {
						tryDispatch(sb);
						sb.setLength(0);
						sb.append(it);
					} else {
						sb.append(it);
						if (tryDispatch(sb)) {
							sb.setLength(0);
						}
					}
				});
			} catch (Exception e) {
				logger.warn("problem fetching events", e);
			} finally {
				try {
					closer.close();
				} catch (IOException e) {
					logger.warn("problem closing", e);
				}
			}
		}

		boolean tryDispatch(StringBuffer sb) {

			ObjectNode n = null;
			try {
				n = (ObjectNode) mapper.readTree(sb.toString());

			} catch (Exception e) {
				return false;
			}
			long nanoTime = n.path("timeNano").asLong();
			if (nanoTime > lastEventNano.get()) {
				lastEventNano.set(Math.max(lastEventNano.get(), nanoTime));
			
				String swarmClusterId = getRestClient().getSwarmClusterId().get();
			

				final ObjectNode data = n;
				Runnable r = new Runnable() {

					@Override
					public void run() {
					
						for (Consumer<JsonNode> consumer : eventConsumers) {
							try {
								consumer.accept(data);
							} catch (RuntimeException e) {
								logger.warn("uncaught exception", e);
							}
						}

					}

				};
				if (dispatchingEnabledSupplier.get()) {
					queue.add(r);
					;
				}

			}
			return true;
		}
	}

	public static String formatNanoTime(long val) {
		return Long.toString(val / 1000000000) + "." + Long.toString(val % 1000000000);

	}

}
