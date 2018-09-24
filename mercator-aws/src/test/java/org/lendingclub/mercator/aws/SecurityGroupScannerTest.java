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

import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.SecurityGroup;

public class SecurityGroupScannerTest {

	@Test
	public void testHash() {
		SecurityGroup sg = new SecurityGroup();
		String h1 = SecurityGroupScanner.getRulesHash(sg);
		Assertions.assertThat(SecurityGroupScanner.getRulesHash(sg)).isEqualTo(h1);

		sg.getIpPermissions().add(new IpPermission().withFromPort(0).withToPort(99)
				.withIpv4Ranges(new IpRange().withCidrIp("0.0.0.0/0")));
		sg.getIpPermissions().add(new IpPermission().withFromPort(123).withToPort(123)
				.withIpv4Ranges(new IpRange().withCidrIp("0.0.0.0/0")));
		String h2 = SecurityGroupScanner.getRulesHash(sg);
		Assertions.assertThat(h2).isNotEqualTo(h1);

		SecurityGroup sg2 = new SecurityGroup();
		// NB - opposite order
		sg2.withIpPermissions(
				new IpPermission().withFromPort(123).withToPort(123)
						.withIpv4Ranges(new IpRange().withCidrIp("0.0.0.0/0")),
				new IpPermission().withFromPort(0).withToPort(99)
						.withIpv4Ranges(new IpRange().withCidrIp("0.0.0.0/0")));
		Assertions.assertThat(SecurityGroupScanner.getRulesHash(sg2)).isEqualTo(h2);
	}

}
