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

import org.lendingclub.mercator.core.BasicProjector;
import org.lendingclub.mercator.core.Projector;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AbstractUnitTest {

	static Projector projector = new Projector.Builder().build();
	
	ObjectMapper mapper = new ObjectMapper();
	
	void prettyPrint(JsonNode n) throws JsonProcessingException{
	
		LoggerFactory.getLogger(VPCScanner.class).info("{}",mapper.writerWithDefaultPrettyPrinter().writeValueAsString(n));
	}
	
	Projector getProjector() {
		return projector;
	}
	
	
}
