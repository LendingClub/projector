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
package org.lendingclub.mercator.bind.model.rdata;

import java.util.HashMap;

import com.google.common.base.Preconditions;

public final class SRVData extends HashMap<String, Object> {
	
	private static final long serialVersionUID = 1L;

	public SRVData (int priority, int weight, int port, String target) {
		Preconditions.checkArgument(priority <= 0xFFFF, "priority must be 0-65535");
		Preconditions.checkArgument(weight <= 0xFFFF, "weight must be 0-65535");
		Preconditions.checkArgument(port <= 0xFFFF, "port must be 0-65535");
		Preconditions.checkNotNull(target, "target");
		
		put("priority", priority);
		put("weight", weight);
	    put("port", port);
	    put("target", target);
	}
	
	public static SRVData.Builder builder() {
	    return new Builder();
	}
	
	public int priority() {
	    return Integer.class.cast(get("priority"));
	}
	
	public int weight() {
	    return Integer.class.cast(get("weight"));
	}
	
	public int port() {
	    return Integer.class.cast(get("port"));
	}
	
	public String target() {
	    return get("target").toString();
	}
	
	public final static class Builder {
		private int priority = -1;
		private int weight = -1;
		private int port = -1;
		private String target;
		
		public SRVData.Builder priority(int priority) {
			this.priority = priority;
			return this;
		}
		
		public SRVData.Builder port(int port) {
			this.port = port;
			return this;
		}
		
		public SRVData.Builder weight(int weight) {
			this.weight = weight;
			return this;
		}
		
		public SRVData.Builder target(String target) {
			this.target = target;
			return this;
		}
		
		public SRVData build() {
			return new SRVData(priority, weight, port, target);
		}
		
	}
}
