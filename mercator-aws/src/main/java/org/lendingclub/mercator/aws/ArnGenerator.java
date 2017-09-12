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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class ArnGenerator {

	Region region;
	String account;
	
	public static ArnGenerator newInstance(String account, Region r) {
		ArnGenerator g = new ArnGenerator();
		g.account = account;
		g.region = r;
		return g;
	}
	public static ArnGenerator newInstance(String account, String region) {
		region=region.toUpperCase().replace("-", "_");
		return newInstance(account, Region.getRegion(Regions.valueOf(region)));
	}
	public static ArnGenerator newInstance(String account, Regions region) {
		return newInstance(account, Region.getRegion(region));
	}

	public String createEc2InstanceArn(String instanceId) {
		return String.format("arn:aws:ec2:%s:%s:instance/%s",region.toString(),account,instanceId);
	}
	
	public String createElbArn(String name) {
		return String.format("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s",region.toString(),account,
			name);
	}
	
	public String createAmiArn(String imageId) {
		return String.format("arn:aws:ec2:%s::image/%s", region.toString(),imageId);
	}
}
