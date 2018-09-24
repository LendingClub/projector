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

import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.neorx.NeoRxClient;

public class DockerSchemaManager extends SchemaManager {

	public DockerSchemaManager(NeoRxClient client) {
		super(client);
		
	}

	@Override
	public void applyConstraints() {

		applyConstraint("CREATE CONSTRAINT ON (a:DockerHost) assert a.swarmNodeId IS UNIQUE ");
		applyConstraint("CREATE CONSTRAINT ON (a:DockerHost) assert a.engineId IS UNIQUE ");
		applyConstraint("CREATE CONSTRAINT ON (a:DockerSwarm) assert a.tridentClusterId IS UNIQUE ");
		applyConstraint("CREATE CONSTRAINT ON (a:DockerSwarm) assert a.swarmClusterId IS UNIQUE ");
		applyConstraint("CREATE CONSTRAINT ON (a:DockerSwarm) assert a.name IS UNIQUE ");

		
		applyUniqueConstraint("DockerService", "serviceId");
		applyUniqueConstraint("DockerTask","taskId");
	}

}
