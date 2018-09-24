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
package org.lendingclub.mercator.aws;

import com.amazonaws.AmazonWebServiceClient;

public abstract class GlobalAWSScanner<T extends AmazonWebServiceClient> extends AWSScanner<T> {

	public GlobalAWSScanner(AWSScannerBuilder builder, Class<T> clientType, String label) {
		super(builder, clientType, label);
	}

	@Override
	protected final void doScan() {
		if (builder.isIncludeGlobalResources()) {
			doGlobalScan();
		}
	}

	protected abstract void doGlobalScan();

}
