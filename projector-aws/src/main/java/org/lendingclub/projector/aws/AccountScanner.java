/**
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
package org.lendingclub.projector.aws;

import org.lendingclub.projector.core.Projector;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.securitytoken.AbstractAWSSecurityTokenService;
import com.google.common.base.Preconditions;

import io.macgyver.neorx.rest.NeoRxClient;

public class AccountScanner extends AbstractEC2Scanner {



	public static final String ACCOUNT_ATTRIBUTE="aws_account";

	public AccountScanner(AWSScannerBuilder builder) {
		super(builder);
		
	}

	@Override
	public void doScan() {
	

			String cypher = "merge (x:AwsAccount {aws_account:{aws_account}}) set x.updateTs=timestamp()";
		
			NeoRxClient neoRx = getNeoRx();
			Preconditions.checkNotNull(neoRx);
			Preconditions.checkNotNull(getAccountId(),"accountId must be set on AWSServiceClient");
			neoRx.execCypher(cypher, ACCOUNT_ATTRIBUTE, getAccountId());
		
	}

}
