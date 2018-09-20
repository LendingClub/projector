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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.NetworkInterface;
import com.amazonaws.services.ec2.model.NetworkInterfaceAssociation;
import com.amazonaws.services.ec2.model.NetworkInterfaceAttachment;
import com.amazonaws.services.ec2.model.Tag;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

public class Neo4jJsonConverterTest {

	Logger logger = LoggerFactory.getLogger(JsonConverter.class);
	ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testEc2Instance() throws IOException {

		Instance instance = new Instance();
		instance.setInstanceId("i-123456");
		instance.setTags(Lists.newArrayList(new Tag("foo", "bar"), new Tag("fizz", "buzz")));

		JsonNode n = new JsonConverter().toJson(instance);
		Assertions.assertThat(n.path("aws_instanceId").asText()).isEqualTo("i-123456");
		Assertions.assertThat(n.path("aws_tag_foo").asText()).isEqualTo("bar");
		Assertions.assertThat(n.path("aws_tag_fizz").asText()).isEqualTo("buzz");
		logger.info("{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n));
	}

	@Test
	public void testNetworkInterface() throws IOException {
		NetworkInterface intf = new NetworkInterface()
				.withAttachment(new NetworkInterfaceAttachment().withAttachmentId("eni-attach-1"))
				.withAssociation(new NetworkInterfaceAssociation().withPublicIp("8.8.8.8"))
				.withNetworkInterfaceId("eni-1").withPrivateIpAddress("192.168.1.1");
		ObjectNode n = new JsonConverter().withFlattenNestedObjects(true).toJson(intf);
		Assertions.assertThat(n.path("aws_attachment_attachmentId").asText()).isEqualTo("eni-attach-1");
		Assertions.assertThat(n.path("aws_association_publicIp").asText()).isEqualTo("8.8.8.8");
		Assertions.assertThat(n.path("aws_networkInterfaceId").asText()).isEqualTo("eni-1");
		logger.info("{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n));
	}

	@Test
	public void testArnGenerator() {
		ArnGenerator arnGenerator = ArnGenerator.newInstance("12345", Regions.US_WEST_2);

		Assertions.assertThat(arnGenerator.createEc2InstanceArn("i-123456"))
				.isEqualTo("arn:aws:ec2:us-west-2:12345:instance/i-123456");

	}

}
