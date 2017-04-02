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

import org.lendingclub.neorx.NeoRxClient;

import com.google.common.base.Preconditions;

public class AccountScanner extends AbstractEC2Scanner {



	public static final String ACCOUNT_ATTRIBUTE="aws_account";

	public AccountScanner(AWSScannerBuilder builder) {
		super(builder);
		setNeo4jLabel("AwsAccount");
		Preconditions.checkNotNull(builder.getProjector());
	}

	@Override
	protected void doScan() {
	

			String cypher = "merge (x:AwsAccount {aws_account:{aws_account}}) set x.updateTs=timestamp()";
		
			NeoRxClient neoRx = getNeoRxClient();
			Preconditions.checkNotNull(neoRx);
			Preconditions.checkNotNull(getAccountId(),"accountId must be set on AWSServiceClient");
			neoRx.execCypher(cypher, ACCOUNT_ATTRIBUTE, getAccountId());
		
	}

}
