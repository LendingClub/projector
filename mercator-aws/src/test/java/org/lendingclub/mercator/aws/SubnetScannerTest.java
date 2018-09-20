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

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.Vpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SubnetScannerTest extends AbstractUnitTest {

	
	@Test
	public void testSerialization() throws JsonProcessingException {
		String account = "123456";
		Regions region = Regions.US_WEST_1;
		
		SubnetScanner scanner = getProjector().createBuilder(AWSScannerBuilder.class).withAccountId(account).withRegion(region).build(SubnetScanner.class);
		
		Subnet subnet = new Subnet();
		subnet.setAvailabilityZone("us-west-2a");
		subnet.setCidrBlock("192.168.100.0/24");
		subnet.setSubnetId("subnet-1234");
		subnet.setVpcId("vpc-9876");
		subnet.setTags(Lists.newArrayList(new Tag("fizz","buzz"),new Tag("foo","bar")));
		JsonNode n = scanner.convertAwsObject(subnet, Region.getRegion(region));
		
		prettyPrint(n);
		Assertions.assertThat(n.path("aws_vpcId").asText()).isEqualTo("vpc-9876");
		Assertions.assertThat(n.path("aws_subnetId").asText()).isEqualTo("subnet-1234");
		Assertions.assertThat(n.path("aws_region").asText()).isEqualTo(region.getName());
		Assertions.assertThat(n.path("aws_account").asText()).isEqualTo(account);
		Assertions.assertThat(n.path("aws_arn").asText()).isEqualTo("arn:aws:ec2:us-west-1:123456:subnet/subnet-1234");
		Assertions.assertThat(n.path("aws_tag_fizz").asText()).isEqualTo("buzz");	
		Assertions.assertThat(n.path("aws_cidrBlock").asText()).isEqualTo("192.168.100.0/24");
		
	}
}
