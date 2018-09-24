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
import org.junit.Test;
import org.lendingclub.mercator.aws.ArnGenerator;

import com.amazonaws.regions.Regions;

public class ArnGeneratorTest {

	@Test
	public void testIt() {

		ArnGenerator g = ArnGenerator.newInstance("123456789", Regions.US_WEST_2);
		
		Assertions.assertThat(g.createElbArn("foo")).isEqualTo("arn:aws:elasticloadbalancing:us-west-2:123456789:loadbalancer/foo");

		Assertions.assertThat(g.createEc2InstanceArn("i-123456")).isEqualTo("arn:aws:ec2:us-west-2:123456789:instance/i-123456");

	}

}
