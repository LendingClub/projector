/**
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
package org.lendingclub.projector.aws;

import java.util.function.Consumer;

import io.macgyver.neorx.rest.NeoRxClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Action1;

import com.amazonaws.regions.Region;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class GraphNodeGarbageCollector {

	
	NeoRxClient neo4j;
	
	Logger logger = LoggerFactory.getLogger(GraphNodeGarbageCollector.class);
	
	long timestamp=Long.MAX_VALUE;
	
	String account;
	String neo4jLabel;
	String region;
	
	public NeoRxClient getNeoRxClient() {
		return neo4j;
	}
	/**
	 * This is a simplistic way to "garbage collect" nodes that have not been updated in the current scan.
	 * It simply looks for nodes matching a given label+account+region tuple that have an updateTs before the given timestamp.
	 * 
	 * This could prove to be problematic if something goes wrong in the scanning process.  A better approach might be a mark-and-sweep
	 * system that marks nodes as being potentially deleted, but then re-attempts locate them in EC2, and only then purge them.
	 * 
	 * This simplistic approach is probably OK for now.
	 * 
	 * @param label
	 * @param account
	 * @param region
	 * @param ts
	 */
	private void invokeNodeGarbageCollector(String label, String account,String region, long ts) {
		
		if (ts==0 || ts==Long.MAX_VALUE) {
			// nothing to do
			return;
		}
		
		Preconditions.checkArgument(!Strings.isNullOrEmpty(label),"label not set");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(account),"account not set");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(region),"region not set");
		
		logger.debug("purging all {} nodes in aws_account={} in region={} updated before {}",label,account,region,ts);
		
		// With neo4j 2.3 we can collapse this into a single DETACH DELETE.  However, for now, we need two operations.

		String cypher = "match (x:"+label+" {aws_account: {account}, aws_region: {region}})-[r]-() where x.updateTs<{ts} detach delete x";
		getNeoRxClient().execCypher(cypher, "account",account,"region",region,"ts",ts);		
	}
	
	public void invoke() {
		invokeNodeGarbageCollector(neo4jLabel, account, region, timestamp);
	}
	public GraphNodeGarbageCollector neo4j(NeoRxClient neo4j) {
		this.neo4j = neo4j;
		return this;
	}
	public GraphNodeGarbageCollector label(String label) {
		this.neo4jLabel=label;
		return this;
	}
	public GraphNodeGarbageCollector account(String account) {
		this.account=account;
		return this;
	}
	public GraphNodeGarbageCollector region(Region region) {
		return region(region.getName());
	}
	public GraphNodeGarbageCollector region(String region) {
		this.region = region;
		return this;
	}
	public GraphNodeGarbageCollector updateEarliestTimestamp(JsonNode n) {
		return updateEarliestTimestamp(n.path("updateTs").asLong(0));
	}
	
	public final Consumer<JsonNode> MERGE_CONSUMER = new Consumer<JsonNode>() {

		@Override
		public void accept(JsonNode t) {
			updateEarliestTimestamp(t.path("updateTs").asLong(0));
		}
	};
	public final Action1<JsonNode> MERGE_ACTION = new Action1<JsonNode>() {


		@Override
		public void call(JsonNode t) {
			updateEarliestTimestamp(t.path("updateTs").asLong(0));		
		}
	};
	public GraphNodeGarbageCollector updateEarliestTimestamp(long l) {
		if (l<=0) {
			return this;
		}
		this.timestamp = Math.min(l, timestamp);
		return this;
	}
}
