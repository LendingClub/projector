/**
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
package org.lendingclub.projector.aws;

import java.util.Optional;

import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import io.macgyver.neorx.rest.NeoRxClient;

public class RegionScanner extends AbstractEC2Scanner {





	public RegionScanner(AWSScannerBuilder builder) {
		super(builder);
	
	}

	@Override
	public Optional<String> computeArn(JsonNode n) {
		return Optional.empty();
	}

	@Override
	public void doScan() {

		GraphNodeGarbageCollector gc = newGarbageCollector().region(getRegion()).label("AwsRegion");

	
		DescribeRegionsResult result = getClient().describeRegions();
		result.getRegions()
				.forEach(
						it -> {
							try {
								ObjectNode n = convertAwsObject(it, getRegion());

								n.remove(AccountScanner.ACCOUNT_ATTRIBUTE);
								String cypher = "merge (x:AwsRegion {aws_regionName:{aws_regionName}}) set x+={props}  remove x.aws_region,x.aws_account set x.updateTs=timestamp() return x";

								NeoRxClient neoRx = getNeoRxClient();
								Preconditions.checkNotNull(neoRx);

								neoRx.execCypher(cypher,
										"aws_regionName",
										n.path("aws_regionName").asText(),
										AWSScanner.AWS_REGION_ATTRIBUTE,
										n.path(AWSScanner.AWS_REGION_ATTRIBUTE).asText(), "props",
										n).forEach(gc.MERGE_ACTION);

							} catch (RuntimeException e) {
								logger.warn("problem scanning regions", e);
							}
						});
		
		gc.invoke();
		
	}

}
