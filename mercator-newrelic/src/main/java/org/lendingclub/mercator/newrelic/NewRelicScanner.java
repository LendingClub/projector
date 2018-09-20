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
package org.lendingclub.mercator.newrelic;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.lendingclub.mercator.core.AbstractScanner;
import org.lendingclub.mercator.core.Scanner;
import org.lendingclub.mercator.core.ScannerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class NewRelicScanner extends AbstractScanner {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private Supplier<NewRelicClient> clientSupplier = Suppliers.memoize(new NewRelicClientSupplier());

	public NewRelicScanner(ScannerBuilder<? extends Scanner> builder) {
		super(builder);
	}

	@Override
	public void scan() {
		
		scanApms();
		scanServers();
		scanAlertPolicies();
		scanAlertConditions();
		purgeOldData();

	}
	
	public NewRelicClient getNewRelicClient() {
		return clientSupplier.get();
	}
	
	/**
	 * Scans all the APM's in NewRelic and establishes relationship to servers
	 * that are hosting them. In this way we know what all servers are hosting 
	 * a particular application.
	 * 
	 */
	private void scanApms() {
		Instant startTime = Instant.now();
		
		ObjectNode apps = getNewRelicClient().getApplications();
		Preconditions.checkNotNull(getProjector().getNeoRxClient(), "neorx client must be set");
		
		String cypher = "WITH {json} as data "
				+ "UNWIND data.applications as app "
				+ "MERGE (a:NewRelicApplication { nr_appId: toString(app.id), nr_accountId: {accountId}} ) "
				+ "ON CREATE SET a.appName = app.name, a.lastReportedAt = app.last_reported_at, a.createTs = timestamp(), a.healthStatus = app.health_status, "
				+ "a.reporting = app.reporting, a.language = app.language, a.updateTs = timestamp() "
				+ "ON MATCH SET a.lastReportedAt = app.last_reported_at, a.reporting = app.reporting, a.healthStatus = app.health_status, a.updateTs = timestamp() "
				+ "FOREACH ( sid IN app.links.servers | MERGE (s:NewRelicServer { nr_serverId: toString(sid), nr_accountId:{accountId} }) MERGE (a)-[:HOSTED_ON]->(s))";


		getProjector().getNeoRxClient().execCypher(cypher, "json", apps, "accountId", clientSupplier.get().getAccountId());
		
		Instant endTime = Instant.now();
		
		logger.info("Updating neo4j with the latest information about {} NewRelic Applications took {} secs", apps.get("applications").size(), Duration.between(startTime, endTime).getSeconds() );

	}
	
	/**
	 * Scans all the servers ( reporting and non-reporting ) in NewRelic.
	 * 
	 */
	private void scanServers() {
		
		Instant startTime = Instant.now();
	
		ObjectNode servers = getNewRelicClient().getServers();
		Preconditions.checkNotNull(getProjector().getNeoRxClient(), "neorx client must be set");
		
		String cypher = "WITH {json} as data "
				+ "UNWIND data.servers as server "
				+ "MERGE ( s:NewRelicServer { nr_serverId: toString(server.id), nr_accountId:{accountId} } ) "
				+ "ON CREATE SET s.name = server.name, s.host = server.host, s.healthStatus = server.health_status, s.reporting = server.reporting, "
				+ "s.lastReportedAt = server.last_reported_at, s.createTs = timestamp(), s.updateTs = timestamp() "
				+ "ON MATCH SET s.name = server.name, s.host = server.host, s.healthStatus = server.health_status, s.reporting = server.reporting, "
				+ "s.lastReportedAt = server.last_reported_at, s.updateTs = timestamp()";

		getProjector().getNeoRxClient().execCypher(cypher, "json", servers, "accountId", clientSupplier.get().getAccountId());
		Instant endTime = Instant.now();
		
		logger.info("Updating neo4j with the latest information about {} NewRelic Servers took {} secs", servers.get("servers").size(), Duration.between(startTime, endTime).getSeconds() );
	}
	
	private void scanAlertPolicies() {
		
		Instant startTime = Instant.now();
		
		ObjectNode alertPolicies = getNewRelicClient().getAlertPolicies();
		Preconditions.checkNotNull(getProjector().getNeoRxClient(), "neorx client must be set");

		String cypher = "WITH {json} as data "
				+ "UNWIND data.policies as policy "
				+ "MERGE ( s:NewRelicAlertPolicy { nr_policyId: toString(policy.id), nr_accountId:{accountId} } ) "
				+ "ON CREATE SET s.name = policy.name, s.policyCreatedTs = policy.created_at, s.policyUpdatedTs = policy.updated_at, "
				+ "s.createTs = timestamp(), s.updateTs = timestamp() "
				+ "ON MATCH SET s.policyUpdatedTs = policy.updated_at, s.updateTs = timestamp() ";

		getProjector().getNeoRxClient().execCypher(cypher, "json", alertPolicies, "accountId", clientSupplier.get().getAccountId());
		Instant endTime = Instant.now();

		logger.info("Updating neo4j with the latest information about {} NewRelic alert policies took {} secs", alertPolicies.get("policies").size(), Duration.between(startTime, endTime).getSeconds() );
	}
	
	private void scanAlertConditions() {
		
		Instant startTime = Instant.now();
		
		String cypher = "MATCH ( p: NewRelicAlertPolicy { nr_accountId:{accountId} }) return p;";
		List<JsonNode> alertPolicies = getProjector().getNeoRxClient().execCypherAsList(cypher, "accountId", clientSupplier.get().getAccountId());
		
		// for each policy get the alert conditions attached		
		alertPolicies.forEach(alert -> {

			String policyId = alert.path("nr_policyId").asText();
			
			if (!Strings.isNullOrEmpty(policyId)) {
				ObjectNode alertConditions = clientSupplier.get().getAlertConditionForPolicy(policyId);
				
				// create relationship between policy and conditions
				// create relationship betwen condition and entities ( servers / apm )				

				String conditionsCypher = "WITH {json} as data "
						+ "UNWIND data.conditions as condition "
						+ "MATCH (p: NewRelicAlertPolicy {nr_policyId: {policyId}, nr_accountId:{accountId}}) "
						+ "MERGE ( c:NewRelicAlertCondition { nr_conditionId: toString(condition.id), nr_accountId: {accountId} }) "
						+ "ON CREATE SET c.type = condition.type, c.name = condition.name, c.enabled = condition.enabled, "
						+ "c.metricType = condition.metric, c.createTs = timestamp(), c.updateTs = timestamp() "
						+ "ON MATCH SET c.updateTs = timestamp(), c.enabled = condition.enabled, c.type = condition.type "
						+ "MERGE (c)-[:BELONGS_TO]->(p) "
						+ "with c,condition, CASE WHEN c.type = 'servers_metric' then [1] ELSE [] END AS aps_var, "
						+ "CASE WHEN c.type = 'apm_app_metric' then [1] ELSE [] END AS apm_var "
						+ "FOREACH ( s IN aps_var | FOREACH(sid in condition.entities | MERGE (s:NewRelicServer { nr_serverId:sid, nr_accountId:{accountId} }) MERGE (s)-[:ATTACHED_TO]->(c))) "
						+ "FOREACH ( m IN apm_var | FOREACH(mid in condition.entities | MERGE (m:NewRelicApplication { nr_appId:mid, nr_accountId:{accountId} }) MERGE (m)-[:ATTACHED_TO]->(c))) ";
				
				getProjector().getNeoRxClient().execCypher(conditionsCypher, "json", alertConditions, "policyId", policyId, "accountId", clientSupplier.get().getAccountId());
			}
		});
		
		

		Instant endTime = Instant.now();
		logger.info("Updating neo4j with the latest information about NewRelic alert conditions took {} secs", Duration.between(startTime, endTime).getSeconds() );
	}
	
	
	/**
	 * NewRelic doesn't remove the non-reporting servers are APM's for ever, 
	 * they just mark it non-reporting. MacGyver cleans up the old non-reporting 
	 * servers from neo4j once in a week. So we might end up with data in Neo4j 
	 * which is neither matched to any server/apm and not updated in neo4j.
	 * 
	 * So we delete the nodes that are not updated for more than a day.
	 * 
	 */
	private void purgeOldData() {
		
		logger.info("Purging old data of NewRelicApplication and NewRelicServer that are removed or cleaned up from NewRelic.");
		long cutoff = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
		getProjector().getNeoRxClient().execCypher("match ( x:NewRelicApplication ) where x.updateTs<{cutoff} detach delete x", "cutoff", cutoff);
		getProjector().getNeoRxClient().execCypher("match ( x:NewRelicServer ) where x.updateTs<{cutoff} detach delete x", "cutoff", cutoff);
		getProjector().getNeoRxClient().execCypher("match ( x:NewRelicAlertPolicy ) where x.updateTs<{cutoff} detach delete x", "cutoff", cutoff);
	}
	
	class NewRelicClientSupplier implements Supplier<NewRelicClient> {

		@Override
		public NewRelicClient get() {
			NewRelicScannerBuilder builder = (NewRelicScannerBuilder) getBuilder();
			return new NewRelicClientImpl(builder.token,builder.accountId);
		}
	}

}
