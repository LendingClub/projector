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

import java.util.List;

import org.lendingclub.mercator.core.ScannerBuilder;

public class BindScannerBuilder extends ScannerBuilder<BindScanner>{
	
	private List<String> zones;
	private String keyName;
	private String keyValue;
	private String keyAlgo;
	private String dnsServer;
	
	public BindScannerBuilder withProperties(String dnsServer, List<String> zones, String keyName, String keyValue, String keyAlgo) {
		this.dnsServer = dnsServer;
		this.keyName = keyName;
		this.keyValue = keyValue;
		this.keyAlgo = keyAlgo;
		this.zones = zones;
		return this;
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

	public void setZones(List<String> zones) {
		this.zones = zones;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public void setKeyValue(String keyValue) {
		this.keyValue = keyValue;
	}

	public void setKeyAlgo(String keyAlgo) {
		this.keyAlgo = keyAlgo;
	}

	public void setDnsServer(String dnsServer) {
		this.dnsServer = dnsServer;
	}
	
	@Override
	public BindScanner build() {
		return new BindScanner(this);
	}
}
