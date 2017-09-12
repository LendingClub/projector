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
package org.lendingclub.mercator.aws;

import java.util.Collections;
import java.util.List;

import org.lendingclub.neorx.NeoRxClient;

import com.google.common.base.Preconditions;

public class LinkageHelper {

	private String fromLabel;
	private String targetLabel;
	private String linkLabel;
	private String targetLinkAttribute = "aws_arn";
	private List<String> targetValues = Collections.emptyList();
	private NeoRxClient neo4j;
	private String fromArn;

	public void execute() {
		
		Preconditions.checkNotNull(fromLabel);
		Preconditions.checkNotNull(targetLabel);
		Preconditions.checkNotNull(linkLabel);
		Preconditions.checkNotNull(targetLinkAttribute);
		Preconditions.checkNotNull(targetValues);
		Preconditions.checkNotNull(neo4j);
		Preconditions.checkNotNull(fromArn);
		
		// create links
		//@formatter:off
		String createCypher = "match (a:" + fromLabel + " { aws_arn: {fromArn} }), "
						+ " (b:" + targetLabel + ")"
						+ " where b." + targetLinkAttribute + " in {targetValues}"
						+ " merge (a)-[r:" + linkLabel + "]->(b) set r.updateTs=timestamp()";
		//@formatter:on
		neo4j.execCypher(createCypher, "fromArn", fromArn, "targetValues", targetValues);
		// remove other links
		//@formatter:off
		String removeCypher = "match (a:" + fromLabel + " { aws_arn: {fromArn} })-[r:" + linkLabel + "]-(b:" + targetLabel + ")"
						+ " where not b." + targetLinkAttribute + " in {targetValues}"
						+ " delete r";
		//@formatter:on
		neo4j.execCypher(removeCypher, "fromArn", fromArn, "targetValues", targetValues);
	}

	public LinkageHelper withFromLabel(String fromLabel) {
		this.fromLabel = fromLabel;
		return this;
	}

	public LinkageHelper withTargetLabel(String toLabel) {
		this.targetLabel = toLabel;
		return this;
	}

	public LinkageHelper withLinkLabel(String linkLabel) {
		this.linkLabel = linkLabel;
		return this;
	}

	public LinkageHelper withNeo4j(NeoRxClient neo4j) {
		this.neo4j = neo4j;
		return this;
	}

	public LinkageHelper withFromArn(String fromArn) {
		this.fromArn = fromArn;
		return this;
	}

	public LinkageHelper withTargetLinkAttribute(String targetLinkAttribute) {
		this.targetLinkAttribute = targetLinkAttribute;
		return this;
	}

	public LinkageHelper withTargetValues(List<String> targetValues) {
		this.targetValues = targetValues;
		return this;
	}

}
