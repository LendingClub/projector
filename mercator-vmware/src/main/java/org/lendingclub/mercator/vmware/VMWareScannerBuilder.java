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
package org.lendingclub.mercator.vmware;

import java.util.Map;

import org.lendingclub.mercator.core.ScannerBuilder;

import com.google.common.collect.Maps;

public class VMWareScannerBuilder extends ScannerBuilder<VMWareScanner> {

	public static final String URL_PROPERTY = "vmware.url";
	public static final String USERNAME_PROPERTY = "vmware.username";
	public static final String PASSWORD_PROPERTY = "vmware.password";
	public static final String VALIDATE_CERTS_PROPERTY = "vmware.validateCerts";

	String url;
	String username;
	String password;
	boolean ignoreCerts = false;

	public VMWareScannerBuilder withUrl(String url) {
		this.url = url;
		return this;
	}

	public VMWareScannerBuilder withUsername(String username) {

		this.username = username;
		return this;
	}

	public VMWareScannerBuilder withPassword(String password) {

		this.password = password;
		return this;
	}

	public VMWareScannerBuilder withIgnoreCerts(boolean b) {
		this.ignoreCerts = b;
		return this;
	}

	@Override
	public VMWareScanner build() {

		return new VMWareScanner(this);

	}

}
