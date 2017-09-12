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

import java.util.List;
import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class S3BucketScanner extends AWSScanner<AmazonS3Client> {

	public S3BucketScanner(AWSScannerBuilder builder) {
		super(builder,AmazonS3Client.class,"AwsS3Bucket");
	}

	@Override
	protected AmazonS3Client createClient() {
		return (AmazonS3Client) builder.configure(AmazonS3ClientBuilder
				.standard()).build();
	}

	@Override
	protected void doScan() {
		AmazonS3Client client = getClient();
		
		rateLimit();
		List<Bucket> bucketList = client.listBuckets();
		
		for (Bucket bucket: bucketList) {
			scanBucket(bucket);
		}
		
	}

	
	@Override
	public Optional<String> computeArn(JsonNode n) {
		String name = n.get("name").asText();
		
		
		return Optional.of("arn:aws:s3:::"+name);
	}

	private void scanBucket(Bucket b) {
	
	
		ObjectNode props = mapper.createObjectNode();
		props.put("name", b.getName());
		
		props.put("aws_arn", computeArn(props).orElse(null));
		props.put("aws_account", getAccountId());
		

		String cypher = "merge (b:AwsS3Bucket { aws_arn:{aws_arn} }) set b+={props}, b.updateTs=timestamp()";
		
		getNeoRxClient().execCypher(cypher, "aws_arn",props.get("aws_arn"), "props",props).forEach(r->{
			getShadowAttributeRemover().removeTagAttributes("AwsS3Bucket", props, r);
		});
		incrementEntityCount();
	
		cypher = "match (a:AwsAccount {aws_account:{account}}), (b:AwsS3Bucket {aws_account:{account}}) MERGE (a)-[r:OWNS]->(b) set r.updateTs=timestamp()";
		
		getNeoRxClient().execCypher(cypher, "account",getAccountId());
		
	
	}
}
