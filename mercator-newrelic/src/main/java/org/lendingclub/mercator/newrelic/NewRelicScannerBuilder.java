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
package org.lendingclub.mercator.newrelic;

import java.util.Map;

import org.lendingclub.mercator.core.ScannerBuilder;

import com.google.common.collect.Maps;

public class NewRelicScannerBuilder extends ScannerBuilder<NewRelicScanner> {

	String token;
	String accountId;

	public NewRelicScannerBuilder withToken(String token) {
		this.token = token;
		return this;
	}

	public NewRelicScannerBuilder withAccountId(String accountId) {
		this.accountId = accountId;
		return this;
	}

	@Override
	public NewRelicScanner build() {
		return new NewRelicScanner(this);
	}

}
