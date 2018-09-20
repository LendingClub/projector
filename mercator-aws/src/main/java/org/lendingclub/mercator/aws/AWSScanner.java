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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.JsonUtil;
import org.lendingclub.mercator.core.MercatorException;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.core.ScanControl;
import org.lendingclub.mercator.core.ScannerContext;
import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.mercator.core.SmartScanner;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.reactivex.exceptions.Exceptions;

public abstract class AWSScanner<T extends AmazonWebServiceClient> extends SmartScanner {

	static ObjectMapper mapper = new ObjectMapper();

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private Projector projector;
	private AmazonWebServiceClient client;
	private Region region;
	ScanControl scanControl;
	String neo4jLabel = null;
	private ScannerMetricCollector metricCollector = new ScannerMetricCollector();
	protected AWSScannerBuilder builder;
	protected JsonConverter jsonConverter = new JsonConverter();

	Class<T> clientType;

	ShadowAttributeRemover shadowRemover;


	

	
	public static final String AWS_REGION_ATTRIBUTE = "aws_region";
	public static final String AWS_ACCOUNT_ATTRIBUTE = "aws_account";
	public static final String AWS_ARN_ATTRIBUTE = "aws_arn";

	public AWSScanner(AWSScannerBuilder builder, Class<T> clientType, String label) {
		super(builder);
		if (builder.getRegion() == null) {
			builder.withRegion(Region.getRegion(Regions.US_EAST_1));
		}
		this.neo4jLabel = label;
		this.clientType = clientType;

		Preconditions.checkNotNull(builder);
		Preconditions.checkNotNull(builder.getRegion());
		Preconditions.checkNotNull(builder.getProjector());

		this.builder = builder;
		this.region = builder.getRegion();
		this.projector = builder.getProjector();

		this.shadowRemover = new ShadowAttributeRemover(getNeoRxClient());

	}

	protected void setNeo4jLabel(String label) {
		this.neo4jLabel = label;
	}

	protected ShadowAttributeRemover getShadowAttributeRemover() {
		return shadowRemover;
	}

	protected T createClient(Class<T> clazz) {
		try {
			String builderClass = clazz.getName() + "Builder";
			Class<?> builderClazz = Class.forName(builderClass);
			Method m = builderClazz.getMethod("standard");
			return clazz.cast(builder.configure((AwsSyncClientBuilder<?, ?>) m.invoke(null)).build());
		} catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException
				| NoSuchMethodException e) {
			throw new MercatorException(e);
		}

	}

	protected T createClient() {
		return (T) createClient(clientType);
	}

	public ScanControl getScanControl() {
		if (scanControl==null) {
			scanControl = new ScanControl(this, getNeo4jLabel());
		}
		return scanControl;
	}
	public Projector getProjector() {
		return projector;
	}

	@SuppressWarnings("unchecked")
	public T getClient() {
		if (this.client == null) {
			this.client = createClient();
		}
		return (T) this.client;
	}

	public Region getRegion() {

		return region;
	}

	public String getAccountId() {
		return (String) builder.getAccountIdSupplier().get();
	}

	public NeoRxClient getNeoRxClient() {
		return projector.getNeoRxClient();
	}

	class AWSScannerContext extends ScannerContext {

		@Override
		protected ToStringHelper toStringHelper() {
			ToStringHelper tsh = super.toStringHelper();
			tsh.add("region", getRegion().getName());
			try {
				tsh.add("account", getAccountId());
			} catch (RuntimeException e) {
				tsh.add("account", "unknown");
			}
			return tsh;
		}

	}

	
	public boolean isSmartScanEnabled() {
		try {
			getNeo4jLabel();
		}
		catch (RuntimeException e) {
			return false;
		}
		return this instanceof AWSSlowScan || super.isSmartScanEnabled();
	}

	
	protected final void doFullScan() {

		new AWSScannerContext().withName(getClass().getSimpleName()).exec(ctx -> {

			try {

				logger.info("{} started scan", toString());
				
				doScan();
			} catch (RuntimeException e) {
				maybeThrow(e);
			}
			markFullScan();
		});

	}

	protected abstract void doScan();

	public ObjectNode convertAwsObject(Object x, Region region) {
		ObjectNode n = jsonConverter.toJson(x);
		if (region != null) {
			n.put(AWS_REGION_ATTRIBUTE, region.getName());
		}
		n.put(AWS_ACCOUNT_ATTRIBUTE, getAccountId());
		Optional<String> arn = computeArn(n);
		if (arn.isPresent()) {
			n.put(AWS_ARN_ATTRIBUTE, arn.get());
		}
		return n;

	}

	public GraphNodeGarbageCollector newGarbageCollector() {
		return new GraphNodeGarbageCollector().neo4j(getNeoRxClient()).account(getAccountId()).region(getRegion())
				.label(getNeo4jLabel());
	}

	public Optional<String> computeArn(JsonNode n) {
		return Optional.empty();
	}

	@Override
	public SchemaManager getSchemaManager() {
		return new AWSSchemaManager(getNeoRxClient());
	}

	public RequestMetricCollector getMetricCollector() {
		return metricCollector;
	}

	public String toString() {
		String safeAccount = "unknown";
		try {
			safeAccount = getAccountId();
		} catch (Exception e) {
			// getAccountId() could trigger a callout...bad thing to happen from
			// toString()
		}
		return MoreObjects.toStringHelper(this).add(AWS_REGION_ATTRIBUTE, getRegion().getName())
				.add(AWS_ACCOUNT_ATTRIBUTE, safeAccount).toString();
	}

	protected boolean tokenHasNext(String token) {
		return (!Strings.isNullOrEmpty(token)) && (!token.equals("null"));

	}

	public String getNeo4jLabel() {
		Preconditions.checkState(neo4jLabel != null);
		return neo4jLabel;
	}

	protected void incrementEntityCount() {
		ScannerContext.getScannerContext().ifPresent(sc -> {
			sc.incrementEntityCount();
		});
	}

	protected String createArn(String service, String region, String entityType, String entityIdentifier) {
		return "arn:aws:" + service + ":" + (region != null ? region : "") + ":" + getAccountId() + ":" + entityType
				+ "/" + entityIdentifier;
	}

	protected String createArn(String service, String entityType, String entityIdentifier) {
		return createArn(service, getRegion().getName(), entityType, entityIdentifier);
	}

	@Override
	public boolean shouldLogStackTrace(Throwable t) {
		try {
			if (t != null && t instanceof SdkClientException && Strings.nullToEmpty(t.getMessage())
					.contains("Unable to load AWS credentials from any provider in the chain")) {
				return false;
			}
			return super.shouldLogStackTrace(t);
		} catch (Exception e) {
			logger.error("programming logic error", e);
		}
		return true;
	}

	protected LinkageHelper newLinkageHelper() {
		return new LinkageHelper().withNeo4j(getNeoRxClient()).withFromLabel(getNeo4jLabel());
	}

	public AWSScannerBuilder newAWSScannerBuilder() {
		return getProjector().createBuilder(AWSScannerBuilder.class).withConfig(this);
	}

	protected AWSScannerBuilder getAWSScannerBuilder() {
		return builder;
	}

	

	/**
	 * Rescan entities in neo4j that have not been updated within a given duration.
	 * @param duration
	 * @param unit
	 */
	protected void rescan(String type) {
		AtomicInteger count = new AtomicInteger();
		String cypher = "match (a:" + type
		+ " {aws_account:{account},aws_region:{region}}) where abs(timestamp()-a.updateTs)>{duration} return a";
		logger.info("cypher: {}",cypher);
		getNeoRxClient().execCypher(cypher,
				"account", getAccountId(), "region", getRegion().getName(), "duration", getEntityScanIntervalMillis())
				.forEach(it -> {
					try {
						count.incrementAndGet();
						rescanEntity(it);
					} catch (RuntimeException e) {
						logger.warn("unexpected exception", e);
					}
				});
		
	}

	protected void rescanEntity(JsonNode data) {
		logger.warn("unsupported operation: rescanEntity() for {}",getClass().getName());
		
		JsonUtil.logInfo("", data);
	}
	
	public long getLastFullScanTs() {
		return getScanControl().getLastScan("region",getRegion().getName(),"account",getAccountId());
	}

	public void markFullScan() {
		if (Strings.isNullOrEmpty(neo4jLabel)) {
			logger.info("cannot record full scan for {}",getClass().getName());
			return;
		}
		logger.info("recording full scan for {}",getNeo4jLabel());
		getScanControl().markLastScan("region",getRegion().getName(),"account",getAccountId());

	}
	
	protected List<JsonNode> findStaleEntities(long duration, TimeUnit unit) {
		return findStaleEntities(getNeo4jLabel(), duration,unit);
	}
	protected List<JsonNode> findStaleEntities(String type, long duration, TimeUnit unit) {
		return getNeoRxClient().execCypher("match (a:" + type + " {aws_account:{account},aws_region:{region}}) where timestamp()-a.updateTs>{threshold} return a",
				"account", getAccountId(), "region", getRegion().getName(),"threshold",unit.toMillis(duration)).toList().blockingGet();

	}



	@Override 
	public boolean isFullScanRequired() {
		if (isSmartScanEnabled()==false) {
			return false;
		}
		long lastFullScanTs = getLastFullScanTs();
		long minsAgo = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-lastFullScanTs);
		long minsToNextFullScan = TimeUnit.MILLISECONDS.toMinutes((lastFullScanTs+getFullScanIntervalMillis())-System.currentTimeMillis());
		if (System.currentTimeMillis()-lastFullScanTs>getFullScanIntervalMillis()) {
			logger.info("full scan required because last scan was {} mins ago",minsAgo);
			
			return true;
		}
		else {
			logger.info("full scan not required for {} mins",minsToNextFullScan);
			return false;
		}

	}

	protected void doSmartScan() {
		logger.warn("doSmartScan() not implemented");
	}


	
	
}
