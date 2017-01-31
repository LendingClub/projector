package org.lendingclub.projector.aws;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lendingclub.projector.core.BasicProjector;
import org.lendingclub.projector.core.Projector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.common.collect.Maps;

import io.macgyver.neorx.rest.NeoRxClient;
import io.macgyver.neorx.rest.NeoRxClientBuilder;

public abstract class AWSIntegrationTest {

	static Logger logger = LoggerFactory.getLogger(AWSIntegrationTest.class);
	static Projector projector;

	synchronized Projector getProjector() {
		if (projector == null) {
		
			Projector p = new BasicProjector(Maps.newHashMap());
			this.projector = p;
		}
		return projector;
	}



	@BeforeClass
	public static void setup() {

		try {
			GetCallerIdentityResult r = AWSSecurityTokenServiceClientBuilder
					.standard().build().getCallerIdentity(new GetCallerIdentityRequest());

			System.out.println(r.getAccount());
			System.out.println(r.getUserId());
			;
			System.out.println(r.getArn());
			Assume.assumeTrue(true);
		} catch (Exception e) {
			logger.warn("AWS Integration tests will be skipped",e);
			Assume.assumeTrue(false);
		}

	}

}
