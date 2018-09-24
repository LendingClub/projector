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
package org.lendingclub.mercator.core;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * Allows scan state to be maintained across runs.  This is primarily used to minimize the amount of full scanning
 * and to implement "smart" scanning support.
 * @author rschoening
 *
 */
public class ScanControl {

	private String type;
	private Scanner scanner;
	private Projector projector;
	private static final String NO_ATTRS="emptyAttributes";
	public ScanControl(Scanner scanner, String type) {
		Preconditions.checkNotNull(scanner);
		Preconditions.checkNotNull(Strings.emptyToNull(type));
		this.scanner = scanner;
		this.type = type;
	}

	@VisibleForTesting
	protected ScanControl(Projector projector, String type) {
		this.projector = projector;
		this.type = type;
	}

	private Projector getProjector() {
		if (projector != null) {
			return projector;
		} else {
			return scanner.getProjector();
		}
	}

	private String toSelector(Map<String, String> m) {
		if (m.containsKey("type")) {
			throw new IllegalArgumentException("attributes cannot contain 'type' key");
		}

		StringBuffer sb = new StringBuffer();
		sb.append("(m:MercatorScan {type:{type}");
		if (m.isEmpty()) {
			sb.append(", "+NO_ATTRS+":true");
		}
		m.forEach((k, v) -> {

			sb.append(", ");
			sb.append(k);
			sb.append(":{");
			sb.append(k);
			sb.append("}");

		});
		sb.append("})");
		return sb.toString();

	}

	private ObjectNode toArgs(Map<String, String> attrs) {
		
		ObjectNode n = ObjectNode.class.cast(JsonUtil.getObjectMapper().valueToTree(attrs)).put("type", type);
		
		if (attrs.isEmpty()) {
			n.put(NO_ATTRS, NO_ATTRS);
		}
		return n;
	}

	private Map<String, String> toMap(String... args) {
		Map<String, String> m = Maps.newHashMap();
		for (int i = 0; i < args.length; i += 2) {
			m.put(args[i], args[i + 1]);
		}
		return m;
	}

	public void markLastScan(String... args) {
		markLastScan(toMap(args));
	}

	public void markForceRescan(String ... args) {
		markForceRescan(toMap(args));
	}
	private void markForceRescan(Map<String, String> attrs) {

		String cypher = "merge " + toSelector(attrs) + " set m.lastScanTs=0";
		
		getProjector().getNeoRxClient().execCypher(cypher,
				toArgs(attrs));
	}
	
	private void markLastScan(Map<String, String> attrs) {

		String cypher = "merge " + toSelector(attrs) + " set m.lastScanTs=timestamp()";
	
		getProjector().getNeoRxClient().execCypher(cypher,
				toArgs(attrs));
	}

	public JsonNode getData(String... args) {
		return getData(toMap(args));
	}

	private JsonNode getData(Map<String, String> attrs) {

		return getProjector().getNeoRxClient().execCypher("match " + toSelector(attrs) + " return m", toArgs(attrs))
				.blockingFirst(NullNode.getInstance());
	}

	public long getLastScan(String... attrs) {
		return getData(attrs).path("lastScanTs").asLong(0);
	}

	public boolean isLastScanWithin(long duration, TimeUnit unit, String... attrs) {
		return getTimeSinceLastScan(attrs) < unit.toMillis(duration);
	}

	public long getTimeSinceLastScan(String... attrs) {
		return System.currentTimeMillis() - getLastScan(attrs);
	}

}
