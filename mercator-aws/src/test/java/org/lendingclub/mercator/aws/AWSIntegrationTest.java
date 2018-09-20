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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.core.Projector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.common.collect.Maps;

public abstract class AWSIntegrationTest {

	static Logger logger = LoggerFactory.getLogger(AWSIntegrationTest.class);
	static Projector projector;

	synchronized Projector getProjector() {
		if (projector == null) {
		
			Projector p = new Projector.Builder().build();
			this.projector = p;
		}
		return projector;
	}



	@BeforeClass
	public static void setup() {

		try {
			GetCallerIdentityResult r = AWSSecurityTokenServiceClientBuilder
					.standard().build().getCallerIdentity(new GetCallerIdentityRequest());

			logger.info("account: {}",r.getAccount());
			logger.info("userId:  {}",r.getUserId());
			logger.info("arn:     {}",r.getArn());
			
			Assume.assumeTrue(true);
		} catch (Exception e) {
			logger.warn("AWS Integration tests will be skipped",e);
			Assume.assumeTrue(false);
		}

	}

}
