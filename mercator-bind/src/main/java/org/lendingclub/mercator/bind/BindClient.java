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
package org.lendingclub.mercator.bind;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Name;
import org.xbill.DNS.TSIG;
import org.xbill.DNS.ZoneTransferException;
import org.xbill.DNS.ZoneTransferIn;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class BindClient {
	
	private List<String> zones;
	private String keyName;
	private String keyValue;
	private String keyAlgo;
	private String dnsServer;

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static class BindBuilder {
		
		List<String> zones;
		String keyName;
		String keyValue;
		String keyAlgo;
		String dnsServer;
		List<String> algosAllowed = Arrays.asList("HMAC_MD5", "HMAC_SHA1", "HMAC_SHA256");
		
		public BindBuilder withProperties(String dnsServer, List<String> zones, String keyName, String keyValue, String keyAlgo) {
			this.dnsServer = dnsServer;
			this.keyName = keyName;
			this.keyValue = keyValue;
			this.keyAlgo = keyAlgo;
			this.zones = zones;
			return this;
		}
	
        public BindClient build() {	
			BindClient client = new BindClient();
			
			Preconditions.checkArgument(zones.size() > 0, "Please specify zones to scan.");
			Preconditions.checkArgument(!Strings.isNullOrEmpty(keyName), "Key Name used to generate TSIG key should not be empty.");
			Preconditions.checkArgument(!Strings.isNullOrEmpty(keyValue), "TSIG key value should not be empty.");
			Preconditions.checkArgument(!Strings.isNullOrEmpty(keyAlgo), "Algorithm used to create TSIG key should not be empty.");
			Preconditions.checkArgument(!Strings.isNullOrEmpty(dnsServer), "DNS Server can not be empty.");
			Preconditions.checkArgument(algosAllowed.contains(keyAlgo), "Algorithm used to create TSIG key should be either HMAC_MD5/HMAC_SHA1/HMAC_SHA256.");
			
			client.dnsServer = dnsServer;
			client.zones = zones;
			client.keyName = keyName;
			client.keyValue = keyValue;
			client.keyAlgo = keyAlgo;

			return client;
		}
	}
	
	private TSIG getTSIGKey() {
	    Name name = null; 	
		switch (keyAlgo) {
		case "HMAC_MD5":
			  name = TSIG.HMAC_MD5;
		      break;
		case "HMAC_SHA256": 
			  name = TSIG.HMAC_SHA256;
		      break;
		case "HMAC_SHA1":
		      name = TSIG.HMAC_SHA1;
		      break;
		default:
			name = TSIG.HMAC_SHA256; 
		}
		
		TSIG tsig = new TSIG(name, keyName, keyValue);
		return tsig;
	}
	
	public Optional<List> getRecordsbyZone(String zone) {
		
		Preconditions.checkArgument(!Strings.isNullOrEmpty(zone), "Zone name can not be empty");
		ZoneTransferIn xfrin;
		try {
			xfrin = ZoneTransferIn.newAXFR(new Name(zone), dnsServer, getTSIGKey());
		
			logger.info("Trying a zone transfer from {}  for {} zone with {} key", dnsServer, zone, keyName);
			return Optional.of(xfrin.run());
		} catch (IOException | ZoneTransferException e) {
			logger.error("Failed to extract the records for {} zone", zone);
		}
		
		return Optional.empty();
	}

	public List<String> getZones() {
		return zones;
	}

	public String getKeyName() {
		return keyName;
	}

	public String getKeyValue() {
		return keyValue;
	}

	public String getKeyAlgo() {
		return keyAlgo;
	}

	public String getDnsServer() {
		return dnsServer;
	}

}
