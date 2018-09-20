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
package org.lendingclub.mercator.docker;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lendingclub.mercator.core.Projector;
import org.lendingclub.mercator.test.MercatorIntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Info;

public abstract class LocalDockerDaemonIntegrationTest extends MercatorIntegrationTest {

	static Logger logger = LoggerFactory.getLogger(LocalDockerDaemonIntegrationTest.class);
	
	static DockerScanner dockerScanner=null;
	

	public DockerScanner getDockerScanner() {
		return dockerScanner;
	}
	@BeforeClass
	public static void setupDockerScanner() {
		DockerScanner ds = new Projector.Builder().build().createBuilder(DockerScannerBuilder.class).withLocalDockerDaemon().build();
		boolean b = ds.getNeoRxClient().checkConnection();
		if (b==false) {
			logger.warn("neo4j is not available...integration tests will be skipped");
			Assume.assumeTrue("neo4j available", b);
		}
		else {
			logger.info("neo4j is available for integration tests");
		}
		
		try {
			Info info = ds.getDockerClient().infoCmd().exec();
			logger.info("local docker daemon is available for integration tests");
			dockerScanner = ds;
		}
		catch (Exception e) {
			logger.warn("neo4j is not available for integration tests",e);
			Assume.assumeTrue(false);
			return;
		}
		
	}
}
