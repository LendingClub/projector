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
package org.lendingclub.mercator.ucs;

import org.lendingclub.mercator.core.ScannerBuilder;

import io.macgyver.okrest3.OkHttpClientConfigurer;
import io.macgyver.okrest3.OkRestClientConfigurer;


public class UCSScannerBuilder extends ScannerBuilder<UCSScanner> {

	String username;
	String password;
	String url;
	boolean validateCertificates=true;
	OkRestClientConfigurer okRestConfig;
	OkHttpClientConfigurer okHttpConfig;
	
	@Override
	public UCSScanner build() {		
		return new UCSScanner(this);
	}


	public UCSScannerBuilder withUrl(String url) {
		this.url = url;
		return this;
	}
	public UCSScannerBuilder withUsername(String username) {
		this.username = username;
		return this;
	}
	public UCSScannerBuilder withPassword(String password) {
		this.password = password;
		return this;
	}
	public UCSScannerBuilder withCertValidationEnabled(boolean b) {
		this.validateCertificates = b;
		return this;
	}
	
	public UCSScannerBuilder withOkRestClientConfig(OkRestClientConfigurer config) {
		this.okRestConfig = config;
		return this;
	}
	public UCSScannerBuilder withOkHttpClientConfig(OkHttpClientConfigurer config) {
		this.okHttpConfig = config;
		return this;
	}
	

	
	
}
