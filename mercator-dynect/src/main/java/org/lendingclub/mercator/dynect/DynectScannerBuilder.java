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
package org.lendingclub.mercator.dynect;

import org.lendingclub.mercator.core.ScannerBuilder;

public class DynectScannerBuilder extends ScannerBuilder<DynectScanner>{
	
	private String companyName;
	private String username;
	private String password;

	public String getCompanyName() {
		return companyName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public DynectScannerBuilder withProperties(String company, String user, String password) {
		this.companyName = company;
		this.username = user;
		this.password = password;
		return this;
	}

	@Override
	public DynectScanner build() {
		return new DynectScanner(this);
	}

}
