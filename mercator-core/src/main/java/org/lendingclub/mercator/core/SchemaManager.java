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
package org.lendingclub.mercator.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.macgyver.neorx.rest.NeoRxClient;

public class SchemaManager {

	Logger logger = LoggerFactory.getLogger(getClass());
	
	NeoRxClient client;
	
	public SchemaManager(NeoRxClient client) {
		this.client = client;
	}
	public NeoRxClient getNeoRxClient() {
		return client;
	}
	public void applyConstraints() {
		
	}
	public void applyConstraint(String constraint) {
		applyConstraint(constraint,false);
	}
	public void applyConstraint(String c, boolean failOnError) {
		try {
			logger.info("applying constraint: {}",c);
			client.execCypher(c);
		} catch (RuntimeException e) {
			if (failOnError) {
				throw e;
			} else {
				logger.warn("problem applying constraints: " + c, e);
			}
		}
	}
}
