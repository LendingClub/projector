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
package org.lendingclub.mercator.vmware;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.lendingclub.mercator.core.MercatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.thoughtworks.proxy.factory.CglibProxyFactory;
import com.thoughtworks.proxy.toys.delegate.DelegationMode;
import com.thoughtworks.proxy.toys.hotswap.HotSwapping;
import com.vmware.vim25.mo.ServiceInstance;

public class ServiceInstanceSupplier implements Supplier<ServiceInstance> {

	public static final String CERTIFICATE_VERIFICATION_DEFAULT = "true";



	static Logger logger = LoggerFactory.getLogger(ServiceInstanceSupplier.class);
	CglibProxyFactory cglibProxyFactory = new CglibProxyFactory();

	static ScheduledExecutorService sharedExecutor = Executors.newScheduledThreadPool(1);

	List<WeakReference<RefreshingServiceInstance>> instances = Lists.newCopyOnWriteArrayList();

	boolean refreshScheduled = false;
	VMWareScannerBuilder scannerBuilder;
	public ServiceInstanceSupplier(VMWareScannerBuilder builder) {
		this.scannerBuilder = builder;
		scheduleRefresh();
	}

	private synchronized void scheduleRefresh() {
		if (!refreshScheduled) {
			logger.info("scheduling VSphere ServiceInstance session token refresh");
			sharedExecutor.scheduleWithFixedDelay(new RefreshTask(), 1, 5, TimeUnit.MINUTES);
			refreshScheduled = true;
		}
	}

	public boolean isRefreshRequired(RefreshingServiceInstance si) {
		try {
			si.currentTime();
			Calendar cal = si.getSessionManager().getCurrentSession().getLoginTime();
			long loginTime = cal.getTimeInMillis();
			long age = System.currentTimeMillis() - loginTime;

			if (age > TimeUnit.MINUTES.toMillis(15)) {
				return true;
			}
		} catch (Exception e) {
			// if the currentTime() call is rejected our session token is likely
			// expired
			return true;
		}

		return false;
	}

	public class RefreshTask implements Runnable {

		@Override
		public void run() {
			try {
				List<WeakReference<RefreshingServiceInstance>> deleteList = Lists.newArrayList();

				for (WeakReference<RefreshingServiceInstance> ref : instances) {
					RefreshingServiceInstance si = ref.get();

					if (si != null) {
						try {
							if (isRefreshRequired(si)) {
								logger.info("refreshing session token: {}", si);
								RefreshingServiceInstance.refreshToken(si);
							}
						} catch (Exception e) {
							logger.warn("problem refreshing session token", e);
						}
					} else {
						deleteList.add(ref);
					}
				}

				instances.removeAll(deleteList);
			} catch (Exception e) {
				logger.warn("uncaught exception", e);
			}
		}
	}

	public ServiceInstance get() {
		try {

			
			logger.info("connecting to {} as {}", scannerBuilder.url, scannerBuilder.username);
			RefreshingServiceInstance si = new RefreshingServiceInstance(new URL(scannerBuilder.url), scannerBuilder.username,
					scannerBuilder.password, scannerBuilder.ignoreCerts);

			final RefreshingServiceInstance cglibProxy = HotSwapping.proxy(RefreshingServiceInstance.class).with(si)
					.mode(DelegationMode.DIRECT).build(cglibProxyFactory);
			addAutoRefresh(cglibProxy);
			return cglibProxy;
		} catch (IOException e) {
			throw new MercatorException(e);
		}

	}

	protected void addAutoRefresh(RefreshingServiceInstance serviceInstance) {
		instances.add(new WeakReference<RefreshingServiceInstance>(serviceInstance));
	}
}
