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

import java.io.IOException;

import org.junit.Test;

public class EC2InstanceScannerTest  {



	@Test
	public void testFlattenInstance() throws IOException {
		String json = "{\n" + "  \"instanceId\" : \"i-abcde1234\",\n" + "  \"imageId\" : \"ami-abcd5544\",\n"
				+ "  \"state\" : {\n" + "    \"code\" : 16,\n" + "    \"name\" : \"running\"\n" + "  },\n"
				+ "  \"privateDnsName\" : \"ip-10-101-1-101.us-west-1.compute.internal\",\n"
				+ "  \"publicDnsName\" : \"\",\n" + "  \"stateTransitionReason\" : \"\",\n"
				+ "  \"keyName\" : \"mykey\",\n" + "  \"amiLaunchIndex\" : 0,\n" + "  \"productCodes\" : [ ],\n"
				+ "  \"instanceType\" : \"c3.large\",\n" + "  \"launchTime\" : 1444437427000,\n"
				+ "  \"placement\" : {\n" + "    \"availabilityZone\" : \"us-west-1b\",\n"
				+ "    \"groupName\" : \"\",\n" + "    \"tenancy\" : \"default\"\n" + "  },\n"
				+ "  \"kernelId\" : null,\n" + "  \"ramdiskId\" : null,\n" + "  \"platform\" : \"windows\",\n"
				+ "  \"monitoring\" : {\n" + "    \"state\" : \"disabled\"\n" + "  },\n"
				+ "  \"subnetId\" : \"subnet-12345678\",\n" + "  \"vpcId\" : \"vpc-12345678\",\n"
				+ "  \"privateIpAddress\" : \"10.101.1.101\",\n" + "  \"publicIpAddress\" : null,\n"
				+ "  \"stateReason\" : null,\n" + "  \"architecture\" : \"x86_64\",\n"
				+ "  \"rootDeviceType\" : \"ebs\",\n" + "  \"rootDeviceName\" : \"/dev/sda1\",\n"
				+ "  \"blockDeviceMappings\" : [ {\n" + "    \"deviceName\" : \"/dev/sda1\",\n" + "    \"ebs\" : {\n"
				+ "      \"volumeId\" : \"vol-12345678\",\n" + "      \"status\" : \"attached\",\n"
				+ "      \"attachTime\" : 1432937430000,\n" + "      \"deleteOnTermination\" : true\n" + "    }\n"
				+ "  } ],\n" + "  \"virtualizationType\" : \"hvm\",\n" + "  \"instanceLifecycle\" : null,\n"
				+ "  \"spotInstanceRequestId\" : null,\n" + "  \"clientToken\" : \"abcdef1234568\",\n"
				+ "  \"tags\" : [ {\n" + "    \"key\" : \"Name\",\n" + "    \"value\" : \"mynode\"\n" + "  } ],\n"
				+ "  \"securityGroups\" : [ {\n" + "    \"groupName\" : \"mygroup\",\n"
				+ "    \"groupId\" : \"sg-abcd1234\"\n" + "  } ],\n" + "  \"sourceDestCheck\" : true,\n"
				+ "  \"hypervisor\" : \"xen\",\n" + "  \"networkInterfaces\" : [ {\n"
				+ "    \"networkInterfaceId\" : \"eni-44444d1d\",\n" + "    \"subnetId\" : \"subnet-61444444\",\n"
				+ "    \"vpcId\" : \"vpc-60e00000\",\n" + "    \"description\" : \"Primary network interface\",\n"
				+ "    \"ownerId\" : \"758521605024\",\n" + "    \"status\" : \"in-use\",\n"
				+ "    \"macAddress\" : \"06:22:22:28:ec:95\",\n" + "    \"privateIpAddress\" : \"10.101.1.101\",\n"
				+ "    \"privateDnsName\" : \"ip-10-101-1-101.us-west-1.compute.internal\",\n"
				+ "    \"sourceDestCheck\" : true,\n" + "    \"groups\" : [ {\n"
				+ "      \"groupName\" : \"QA_Group\",\n" + "      \"groupId\" : \"sg-26100000\"\n" + "    } ],\n"
				+ "    \"attachment\" : {\n" + "      \"attachmentId\" : \"eni-attach-8ef00000\",\n"
				+ "      \"deviceIndex\" : 0,\n" + "      \"status\" : \"attached\",\n"
				+ "      \"attachTime\" : 1432937427000,\n" + "      \"deleteOnTermination\" : true\n" + "    },\n"
				+ "    \"association\" : null,\n" + "    \"privateIpAddresses\" : [ {\n"
				+ "      \"privateIpAddress\" : \"10.101.1.101\",\n"
				+ "      \"privateDnsName\" : \"ip-10-101-1-101.us-west-1.compute.internal\",\n"
				+ "      \"primary\" : true,\n" + "      \"association\" : null\n" + "    } ]\n" + "  } ],\n"
				+ "  \"iamInstanceProfile\" : null,\n" + "  \"ebsOptimized\" : false,\n"
				+ "  \"sriovNetSupport\" : null\n" + "}";

	}

	/*
	@Test
	public void testScanRegion() {
		
		AmazonEC2Client ec2 = Mockito.mock(AmazonEC2Client.class);
		
		AWSServiceClient c = newMockServiceClient();
		
		Mockito.when(c.createEC2Client(Region.getRegion(Regions.US_WEST_2))).thenReturn(ec2);
		
		DescribeInstancesResult r = new DescribeInstancesResult();
		
		List<Reservation> rlist = Lists.newArrayList();
		Reservation res = new Reservation();
		rlist.add(res);
		r.setReservations(rlist);
		
		Instance instance = new Instance();
		instance.setInstanceId("i-123456");
		res.setInstances(Lists.newArrayList(instance));
		instance.setSubnetId("subnet-1234");
		Mockito.when(ec2.describeInstances(new DescribeInstancesRequest())).thenReturn(r);
		EC2InstanceScanner scanner = new EC2InstanceScanner(c, neo4j);
		scanner.scan("us-west-2");
	}
	
	@Test
	public void testAttributes() {


		EC2InstanceScanner scanner = new EC2InstanceScanner(newMockServiceClient(), neo4j);
		
	
		Instance instance = new Instance();
		instance.setImageId("i-1234");
		instance.setInstanceId("i-123456");
		ObjectNode n = scanner.convertAwsObject(instance, Region.getRegion(Regions.US_WEST_2));
		
		List<String> fields = Lists.newArrayList(n.fieldNames());
		
		
		Assertions.assertThat(fields).contains("aws_instanceId","aws_imageId","aws_privateDnsName","aws_subnetId","aws_arn",ACCOUNT_ATTRIBUTE,AWS_REGION);
		
		Assertions.assertThat(n.path("aws_imageId").asText()).isEqualTo("i-1234");
	
	}

	@Test
	public void testComputeArn() {

		AWSServiceClient c = newMockServiceClient();

		EC2InstanceScanner scanner = new EC2InstanceScanner(c, neo4j);

		ObjectNode n = mapper.createObjectNode();
		n.put(ACCOUNT_ATTRIBUTE, "123456789012");
		n.put(AWS_REGION, "us-west-2");
		n.put("aws_instanceId", "i-123456");

		Assertions.assertThat(scanner.computeArn(n).get())
				.isEqualTo("arn:aws:ec2:us-west-2:123456789012:instance/i-123456");

		Assertions.assertThat(scanner.getAccountId()).isEqualTo(getAccountId());

	}*/

}
