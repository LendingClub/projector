package org.lendingclub.mercator.aws;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
		private int scanNumber;
		private Map<Class<? extends AWSScanner<?>>, Double> rateLimits;
		private Map<Class<? extends AWSSlowScan>, int[]> slowScanRatios;
		private Class<?> scannerTypeUnderConstruction;

		public TargetBuilder(Builder builder, int scanNumber) {
			super(builder);
			this.rateLimits = builder.rateLimits;
			this.slowScanRatios = builder.slowScanRatios;
			this.scanNumber = scanNumber;
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
				int[] ratio = slowScanRatios.get(slowScanner.getClass());
				if (ratio == null) {
					ratio = slowScanner.getSlowScanRatio();
				}
				if (((ratio[0] * scanNumber) % ratio[1]) != 0) {
					return null;
				}
				logger.info("including slow " + scanner.getClass().getSimpleName() + " on this iteration");
			}
			return scanner;
		}

	}

	public static class Builder extends AWSScannerBuilder {

		List<Target> targets = new ArrayList<>();
		Regions globalRegion;
		List<Class<? extends AWSScanner<?>>> disabledScanTypes = new ArrayList<>();
		Map<Class<? extends AWSScanner<?>>, Double> rateLimits = new HashMap<>();
		Map<Class<? extends AWSSlowScan>, int[]> slowScanRatios = new HashMap<>();

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

		public Builder withRateLimit(Class<? extends AWSScanner<?>> scannerType, double rateLimitPerSecond) {
			rateLimits.put(scannerType, rateLimitPerSecond);
			return this;
		}

		public Builder withSlowScanRatio(Class<? extends AWSSlowScan> scannerType, int[] scanRatio) {
			slowScanRatios.put(scannerType, scanRatio);
			return this;
		}
		
		public Builder defaultSlowScanRatios() {
			slowScanRatios.clear();
			return this;
		}

		public Builder defaultRateLimits() {
			rateLimits.clear();
			return this;
		}

		public Builder defaultDisabledScanTypes() {
			disabledScanTypes.clear();
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

		AWSScannerBuilder createTargetBuilder(int scanNumber, Target target) {
			AWSScannerBuilder builder = new TargetBuilder(this, scanNumber).withCredentials(target.credentialsProvider)
					.withRegion(target.region).withIncludeGlobalResources(globalRegion == target.region);
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

	private int scanNumber;

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
			AllEntityScanner regionScanner = builder.createTargetBuilder(scanNumber, target)
					.build(AllEntityScanner.class);
			for (Class<? extends AWSScanner<?>> disabledScanType : builder.disabledScanTypes) {
				regionScanner.removeScannerType(disabledScanType);
			}
			result.add(regionScanner);
		}
		scanNumber += 1;
		return result;
	}

}
