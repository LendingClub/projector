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
package org.lendingclub.mercator.kubernetes;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.ReplicaSet;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;

public class KubeEventScanner {
	static org.slf4j.Logger logger = LoggerFactory.getLogger(KubeEventScanner.class);

	static ExecutorService executor = Executors.newFixedThreadPool(10,
			new ThreadFactoryBuilder().setDaemon(true).setNameFormat("k8s-evt-%d").build());

	KubeScanner scanner;
	AtomicReference<Supplier<Boolean>> enabledRef = new AtomicReference<>(ALWAYS);
	static ObjectMapper mapper = new ObjectMapper();
	java.util.List<Consumer<Single<KubeEvent>>> subscribers = Lists.newCopyOnWriteArrayList();
	private static Supplier<Boolean> ALWAYS = new Supplier<Boolean>() {

		@Override
		public Boolean get() {
			return true;
		}

	};

	KubeEventScanner(KubeScanner scanner) {
		this.scanner = scanner;
	}

	public KubeEventScanner watchAll() {

	
		watchServices();
		watchDeployments();
		watchEndpoints();
		watchEvents();
		
		watchNamespaces();
		watchNodes();

	

		logger.info("watch all complete");
		return this;
	}

	protected void dispatch(Action action, HasMetadata resource) {
		KubeEvent evt = new KubeEvent(scanner.getClusterId(),scanner.getClusterName(),action, resource);

		subscribers.forEach(it -> {

			try {
				it.accept(Single.just(evt));
			} catch (Exception e) {
				logger.warn("problem", e);
			}
		});
	}

	public boolean isScannerEnabled() {
		boolean b = enabledRef.get().get();
		return b;
	}

	public KubeEventScanner watchServices() {
		Stopwatch sw = Stopwatch.createStarted();

		Watcher<Service> serviceWatch = new Watcher<Service>() {

			@Override
			public void eventReceived(Action action, Service resource) {
				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanServices(resource.getMetadata().getNamespace(),
									resource.getMetadata().getName());
						}
						dispatch(action, resource);

					}

				});

			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);
			}

		};
		Watch watch = scanner.getKubernetesClient().services().inAnyNamespace().watch(serviceWatch);
		registerWatch(watch,Service.class, sw);
		return this;
	}

	public KubeEventScanner watchNamespaces() {

		Stopwatch sw = Stopwatch.createStarted();
		Watcher<Namespace> watcher = new Watcher<Namespace>() {

			@Override
			public void eventReceived(Action action, Namespace resource) {

				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanNamespaces(resource.getMetadata().getName());
						}
						dispatch(action, resource);

					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);
			}

		};
		Watch watch = scanner.getKubernetesClient().namespaces().watch(watcher);

		registerWatch(watch,Namespace.class, sw);
		return this;
	}

	/**
	 * Enables the event -> scan invocation to be selectively enabled/disabled. The
	 * primary use-case for this is making sure that in an horizontally scaled
	 * cluster, that scanning is not performed on EACH node. That is, each node may
	 * be watching Kubernetes eveents, but only one node needs to update
	 * 
	 * @param b
	 * @return
	 */
	public KubeEventScanner withIncrementalScanEnabled(boolean b) {
		Supplier<Boolean> supplier = new Supplier<Boolean>() {

			@Override
			public Boolean get() {
				return b;
			}
		};
		return withIncrementalScanEnabled(supplier);
	}

	/**
	 * Enables the event -> scan invocation to be selectively enabled/disabled. The
	 * primary use-case for this is making sure that in an horizontally scaled
	 * cluster, that scanning is not performed on EACH node. That is, each node may
	 * be watching Kubernetes events, but only one node needs to update
	 * 
	 * @param b
	 * @return
	 */
	public KubeEventScanner withIncrementalScanEnabled(Supplier<Boolean> supplier) {
		enabledRef.set(supplier);
		return this;
	}

	public KubeEventScanner withRxSingle(Consumer<Single<KubeEvent>> c) {

		this.subscribers.add(c);
		return this;
	}

	public KubeEventScanner withConsumer(Consumer<KubeEvent> consumer) {
		return addConsumer(consumer);
	}

	public KubeEventScanner addConsumer(Consumer<KubeEvent> hook) {
		Consumer<Single<KubeEvent>> x = new Consumer<Single<KubeEvent>>() {
			@Override
			public void accept(Single<KubeEvent> t) throws Exception {
				t.subscribe(hook);
			}
		};
		this.subscribers.add(x);

		return this;
	}

	private void log(Action action, HasMetadata md, KubeScanner scanner) {
		logger.info("{} type={} clusterId={} clusterName={} namespace={} name={}", action, md.getKind(),
				scanner.getClusterId(), scanner.getClusterName(), md.getMetadata().getNamespace(),
				md.getMetadata().getName());
	}

	
	public KubeEventScanner watchEndpoints() {

		Stopwatch sw = Stopwatch.createStarted();
		Watcher<Endpoints> watcher = new Watcher<Endpoints>() {

			@Override
			public void eventReceived(Action action, Endpoints resource) {
				
				if (action == Action.MODIFIED
						&& Strings.nullToEmpty(resource.getMetadata().getNamespace()).equals("kube-system")
						&& ImmutableList.of("kube-controller-manager", "kube-scheduler")
								.contains(Strings.nullToEmpty(resource.getMetadata().getName()))) {
					return;
				}
				executor.execute(new Runnable() {

					@Override
					public void run() {

						log(action, resource, scanner);

						if (isScannerEnabled()) {
							scanner.scanEndpoints(resource.getMetadata().getNamespace(),
									resource.getMetadata().getName());
						}
						dispatch(action, resource);

					}
				});

			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);

			}

		};
		Preconditions.checkNotNull(scanner);
		Watch watch = scanner.getKubernetesClient().endpoints().inAnyNamespace().watch(watcher);
		registerWatch(watch,Endpoints.class, sw);
		return this;
	}

	public KubeEventScanner watchDeployments() {
		Stopwatch sw = Stopwatch.createStarted();
		Watcher<Deployment> watcher = new Watcher<Deployment>() {

			@Override
			public void eventReceived(Action action, Deployment resource) {

				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanDeployments(resource.getMetadata().getNamespace(),
									resource.getMetadata().getName());
						}
						dispatch(action, resource);
					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);
			}

		};
		Watch watch = scanner.getKubernetesClient().extensions().deployments().inAnyNamespace().watch(watcher);
		registerWatch(watch,Deployment.class, sw);
		return this;
	}

	public KubeEventScanner watchEvents() {
		Stopwatch sw = Stopwatch.createStarted();

		Watcher<Event> watcher = new Watcher<Event>() {

			@Override
			public void eventReceived(Action action, Event event) {
				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, event, scanner);
						if (isScannerEnabled()) {
							try {
								String involvedObjectType = Strings.nullToEmpty(event.getInvolvedObject().getKind());
								String involvedObjectName = event.getInvolvedObject().getName();
								String involvedObjectNamespace = event.getInvolvedObject().getNamespace();

								if (involvedObjectName != null && involvedObjectNamespace != null) {
									if (involvedObjectType.equals("Pod")) {
										scanner.scanPods(involvedObjectNamespace, involvedObjectName);
									} else if (involvedObjectType.equals("ReplicaSet")) {
										scanner.scanReplicaSets(involvedObjectNamespace, involvedObjectName);
									} else if (involvedObjectType.equals("Deployment")) {
										scanner.scanDeployments(involvedObjectNamespace, involvedObjectName);
									}
								}

							} catch (RuntimeException e) {
								logger.warn("problem interpreting event", e);
							}

						}
						dispatch(action, event);

					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);
			}

		};

		Watch watch = scanner.getKubernetesClient().events().watch(watcher);
		registerWatch(watch,Event.class, sw);
		return this;
	}

	private KubeEventScanner watchReplicaSets_BROKEN() {

		Stopwatch sw = Stopwatch.createStarted();

		Watcher<ReplicaSet> watcher = new Watcher<ReplicaSet>() {

			@Override
			public void eventReceived(Action action, ReplicaSet resource) {
				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanNodes(resource.getMetadata().getName());
						}
						dispatch(action, resource);

					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);
			}

		};

		Watch watch = scanner.getKubernetesClient().extensions().replicaSets().watch(watcher);
		registerWatch(watch,ReplicaSet.class, sw);
		return this;
	}

	private void registerWatch(Watch watch, Class type, Stopwatch sw) {
		List<String> tmp = Splitter.on(".").splitToList(type.getName());
		logger.info("{} watch registration for {} took {} ms", tmp.get(tmp.size() - 1), scanner.getClusterName(),sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private KubeEventScanner watchPods_BROKEN() {

		Stopwatch sw = Stopwatch.createStarted();
		Watcher<Pod> watcher = new Watcher<Pod>() {

			@Override
			public void eventReceived(Action action, Pod resource) {
				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanPods(resource.getMetadata().getName());
						}
						dispatch(action, resource);

					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);

			}

		};
		// for whatever reason, registering a watcher on pods is slow

		Watch watch = scanner.getKubernetesClient().pods().watch(watcher);
		registerWatch(watch,Pod.class, sw);
		return this;
	}

	private void register(Runnable r) {
		new Thread(r).start();
	}

	public KubeEventScanner watchNodes() {
		Stopwatch sw = Stopwatch.createStarted();
		Watcher<Node> watcher = new Watcher<Node>() {

			@Override
			public void eventReceived(Action action, Node resource) {

				executor.execute(new Runnable() {

					@Override
					public void run() {
						log(action, resource, scanner);
						if (isScannerEnabled()) {
							scanner.scanNodes(resource.getMetadata().getName());
						}
						dispatch(action, resource);
					}
				});
			}

			@Override
			public void onClose(KubernetesClientException cause) {
				logger.info("watch closed", cause);

			}

		};

		Watch watch = scanner.getKubernetesClient().nodes().watch(watcher);
		registerWatch(watch,Node.class, sw);
		return this;
	}
}
