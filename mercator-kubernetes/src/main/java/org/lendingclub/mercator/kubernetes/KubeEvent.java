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
package org.lendingclub.mercator.kubernetes;

import com.google.common.base.MoreObjects;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.Watcher.Action;

public class KubeEvent {
	String sourceClusterId;
	String sourceClusterName;
	Action action;
	HasMetadata resource;

	public KubeEvent(String clusterId, String clusterName, Action action, HasMetadata resource) {
		this.sourceClusterId = clusterId;
		this.sourceClusterName = clusterName;
		this.action = action;
		this.resource = resource;
	}

	public String toString() {
		try {
			return MoreObjects.toStringHelper(this).add("action", action).add("resourceType", resource.getKind())
					.add("clusterName", sourceClusterName).toString();
		} catch (Exception e) {
			return super.toString();
		}
	}

	public String getClusterName() {
		return sourceClusterName;
	}

	public String getClusterId() {
		return sourceClusterId;
	}

	public Action getAction() {
		return action;
	}

	public HasMetadata getResource() {
		return resource;
	}
}
