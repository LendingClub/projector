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

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.google.common.base.Preconditions;

public abstract class AbstractEC2Scanner extends AWSScanner<AmazonEC2Client> {

	public AbstractEC2Scanner(AWSScannerBuilder builder, String label) {
		super(builder, AmazonEC2Client.class, label);
		Preconditions.checkNotNull(builder.getProjector());
	}

	protected String createEc2Arn(String entityType, String entityIdentifier) {
		return createArn("ec2", entityType, entityIdentifier);
	}

}
