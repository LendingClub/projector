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

package org.lendingclub.mercator.aws;

import java.util.Optional;

import org.lendingclub.neorx.NeoRxClient;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class AMIScanner extends AbstractEC2Scanner {


	public static final String IMAGE_ID_ATTRIBUTE="aws_imageId";

	public AMIScanner(AWSScannerBuilder builder) {
		super(builder);
		setNeo4jLabel("AwsAmi");
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		
		String region = n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(null);
		String imageId = n.path(IMAGE_ID_ATTRIBUTE).asText(null);
		
		Preconditions.checkState(!Strings.isNullOrEmpty(region), "aws_region not set");
		Preconditions.checkState(!Strings.isNullOrEmpty(imageId), "aws_imageId not set");

		return Optional.of(String.format("arn:aws:ec2:%s::image/%s", region, imageId));
	}

	@Override
	protected void doScan() {
		AmazonEC2Client c = getClient();
		
		NeoRxClient neoRx = getNeoRxClient();
		Preconditions.checkNotNull(neoRx);
		GraphNodeGarbageCollector gc = newGarbageCollector().bindScannerContext();
		DescribeImagesRequest req = new DescribeImagesRequest().withOwners("self");
		DescribeImagesResult result = c.describeImages(req);
		
		result.getImages().forEach(i -> { 
			try { 
				ObjectNode n = convertAwsObject(i, getRegion());
			
				
				String cypher = "merge (x:AwsAmi {aws_arn:{aws_arn}}) set x+={props} set x.updateTs=timestamp() return x";
				neoRx.execCypher(cypher, "aws_arn", n.path("aws_arn").asText(), "props",n).forEach( r->{
					gc.MERGE_ACTION.accept(r);
					getShadowAttributeRemover().removeTagAttributes("AwsAmi", n, r);
				});
				incrementEntityCount();
			} catch (RuntimeException e) { 
				maybeThrow(e,"problem scanning AMI "+i.getImageId());
			}
		});
	
	}

}
