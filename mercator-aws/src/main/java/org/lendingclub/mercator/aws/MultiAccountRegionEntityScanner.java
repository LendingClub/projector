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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.lendingclub.mercator.core.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.common.base.Strings;

/**
 * A scanner that scans all entities in multiple accounts and regions in
 * parallel.
 */
public class MultiAccountRegionEntityScanner extends AWSParallelScannerGroup {

	private static class Target {
		final String accountId;
		final AWSCredentialsProvider credentialsProvider;
		final Regions region;

		Target(String accountId, AWSCredentialsProvider credentialsProvider, Regions region) {
			this.accountId = accountId;
			this.credentialsProvider = credentialsProvider;
			this.region = region;
		}
	}

	private static class TargetBuilder extends AWSScannerBuilder {
		private static final Logger logger = LoggerFactory.getLogger(MultiAccountRegionEntityScanner.class);
		private Map<Class<?>, Double> rateLimits;
		private Class<?> scannerTypeUnderConstruction;
		private Map<String, Long> lastScanTimes;
		private long now;
		private Map<Class<?>, Long> slowScanIntervals;

		public TargetBuilder(Builder builder, Map<String, Long> lastScanTimes) {
			super(builder);
			this.rateLimits = builder.rateLimits;
			this.slowScanIntervals = builder.slowScanIntervals;
			this.lastScanTimes = lastScanTimes;
			this.now = System.currentTimeMillis();
		}

		@Override
		public Optional<Double> getRateLimitPerSecond() {
			if (scannerTypeUnderConstruction != null) {
				Double limit = rateLimits.get(scannerTypeUnderConstruction);
				if (limit != null) {
					return Optional.of(limit);
				}
			}
			return Optional.empty();
		}

		@SuppressWarnings("rawtypes")
		@Override
		public <T extends AWSScanner> T build(Class<T> clazz) {
			T scanner;
			synchronized (this) {
				try {
					scannerTypeUnderConstruction = clazz;
					scanner = super.build(clazz);
				} finally {
					scannerTypeUnderConstruction = null;
				}
			}
			if (scanner instanceof AWSSlowScan) {
				AWSSlowScan slowScanner = (AWSSlowScan) scanner;
				String key = clazz.getName() + ":" + scanner.getRegion() + ":" + scanner.getAccountId();
				long lastScanTime = lastScanTimes.getOrDefault(key, 0L);
				long minimumScanInterval = slowScanIntervals.getOrDefault(clazz, slowScanner.getMinimumScanInterval());
				if ((now - lastScanTime) < minimumScanInterval) {
					return null;
				}
				logger.info("including slow scan " + key);
				lastScanTimes.put(key, now);
			}
			return scanner;
		}

	}

	public static class Builder extends AWSScannerBuilder {

		List<Target> targets = new ArrayList<>();
		Regions globalRegion;
		Set<Class<?>> disabledScanTypes = new HashSet<>();
		Map<Class<?>, Double> rateLimits = new HashMap<>();
		Map<Class<?>, Long> slowScanIntervals = new HashMap<>();

		public Builder() {
		}

		public Builder withAccountAndRegions(String accountId, AWSCredentialsProvider credentialsProvider,
				List<Regions> regions) {
			for (Regions region : regions) {
				targets.add(new Target(accountId, credentialsProvider, region));
			}
			return this;
		}

		public Builder withDisabledScanType(Class<? extends AWSScanner<?>> scannerType) {
			disabledScanTypes.add(scannerType);
			return this;
		}

		public Builder withDisabledScanTypes(Collection<Class<?>> scannerTypes) {
			disabledScanTypes.clear();
			if (scannerTypes != null) {
				disabledScanTypes.addAll(scannerTypes);
			}
			return this;
		}

		public Builder withRateLimits(Map<Class<?>, ? extends Number> rateLimits) {
			this.rateLimits.clear();
			if (rateLimits != null) {
				rateLimits.forEach((k, v) -> this.rateLimits.put(k, v.doubleValue()));
			}
			return this;
		}
		

		public Builder withSlowScanIntervals(Map<Class<?>, Number> slowScanIntervals) {
			this.slowScanIntervals.clear();
			if (slowScanIntervals != null) {
				slowScanIntervals.forEach((k,v) -> this.slowScanIntervals.put(k, v.longValue()));
			}
			return this;
		}


		@SuppressWarnings({ "rawtypes" })
		@Override
		public <T extends AWSScanner> T build(Class<T> clazz) {
			throw new IllegalStateException("this builder cannot be used to construct scanners");
		}

		@Override
		public MultiAccountRegionEntityScanner build() {
			return (MultiAccountRegionEntityScanner) super.build(MultiAccountRegionEntityScanner.class);
		}

		AWSScannerBuilder createTargetBuilder(Target target, Map<String, Long> lastScanTimes) {
			AWSScannerBuilder builder = new TargetBuilder(this, lastScanTimes)
					.withCredentials(target.credentialsProvider).withRegion(target.region)
					.withIncludeGlobalResources(globalRegion == target.region);
			if (!Strings.isNullOrEmpty(target.accountId)) {
				builder.withAccountId(target.accountId);
			}
			return builder;
		}

		public Builder withGlobalRegion(Regions globalRegion) {
			this.globalRegion = globalRegion;
			return this;
		}

	}

	private Map<String, Long> lastScanTimes = new HashMap<>();

	public MultiAccountRegionEntityScanner(AWSScannerBuilder builder) {
		super(builder);
		if (!(builder instanceof Builder)) {
			throw new IllegalArgumentException("this scanner must only be constructed with its specific builder");
		}
	}

	@Override
	public Region getRegion() {
		// just return something, it's really N/A
		return Region.getRegion(Regions.US_EAST_1);
	}

	@Override
	public String getAccountId() {
		return "<multiple>";
	}

	@Override
	protected List<Scanner> getScanners() {
		Builder builder = (Builder) this.builder;
		List<Scanner> result = new ArrayList<>();
		for (Target target : ((Builder) builder).targets) {
			AllEntityScanner regionScanner = builder.createTargetBuilder(target, lastScanTimes)
					.build(AllEntityScanner.class);
			for (Class<?> disabledScanType : builder.disabledScanTypes) {
				regionScanner.removeScannerType(disabledScanType);
			}
			result.add(regionScanner);
		}
		return result;
	}

}
