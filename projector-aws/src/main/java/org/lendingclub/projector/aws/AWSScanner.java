package org.lendingclub.projector.aws;

import java.util.Optional;


import org.lendingclub.projector.core.Projector;
import org.lendingclub.projector.core.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import io.macgyver.neorx.rest.NeoRxClient;

public abstract class AWSScanner<T extends AmazonWebServiceClient> implements Scanner {

	static ObjectMapper mapper = new ObjectMapper();

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private Projector projector;
	private AmazonWebServiceClient client;
	private Region region;

	protected AWSScannerBuilder builder;

	public static final String AWS_REGION_ATTRIBUTE="aws_region";

	public AWSScanner(AWSScannerBuilder builder) {
		if (builder.region==null) {
			builder.region = Region.getRegion(Regions.US_EAST_1);
		}
		
		Preconditions.checkNotNull(builder);
		Preconditions.checkNotNull(builder.region);
		Preconditions.checkNotNull(builder.projector);
		
		this.builder = builder;
		this.region = builder.region;
		this.projector = builder.projector;
		

	}


	protected abstract T createClient();
	
	@SuppressWarnings("unchecked")
	public T getClient() {
		if (this.client==null) {
			this.client = createClient();
		}
		return (T) this.client;
	}

	public Region getRegion() {
		
		return region;
	}

	public String getAccountId() {
		return builder.acccountIdSupplier.get();
	}

	public NeoRxClient getNeoRx() {
		return getNeoRxClient();
	}
	public NeoRxClient getNeoRxClient() {
		return projector.getNeoRx();
	}
	public final void scan() {
	
		long t0 = System.currentTimeMillis();
		logger.info("start scan - account={} region={}",getAccountId(),getRegion());
		doScan();
		long t1 = System.currentTimeMillis();
		logger.info("end scan - account={} region={} duration={} ms",getAccountId(),getRegion(),t1-t0);
	}
	public abstract void doScan();

	public ObjectNode convertAwsObject(Object x, Region region) {
		ObjectNode n = mapper.valueToTree(x);
		n.put("region", region.getName());
		n.put("account", getAccountId());
		n = flatten(n);
		return n;
	}

	public GraphNodeGarbageCollector newGarbageCollector() {
		return new GraphNodeGarbageCollector().neo4j(getNeoRx()).account(getAccountId());
	}

	public Optional<String> computeArn(JsonNode n) {

		return Optional.empty();

	}

	protected ObjectNode flatten(ObjectNode n) {
		ObjectNode r = mapper.createObjectNode();

		n.fields().forEachRemaining(it -> {

			if (!it.getValue().isContainerNode()) {
				r.set("aws_" + it.getKey(), it.getValue());
			}

		});

		n.path("tags").iterator().forEachRemaining(it -> {
			String tagKey = "aws_tag_" + it.path("key").asText();
			r.put(tagKey, it.path("value").asText());
		});

		Optional<String> arn = computeArn(r);
		if (arn.isPresent()) {
			r.put("aws_arn", arn.get());
		}

		return r;
	}

}
