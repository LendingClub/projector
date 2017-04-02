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

import org.lendingclub.mercator.core.SchemaManager;
import org.lendingclub.neorx.NeoRxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSSchemaManager extends SchemaManager {

	Logger logger = LoggerFactory.getLogger(AWSSchemaManager.class);




	public AWSSchemaManager(NeoRxClient client) {
		super(client);
		
	}




	public void applyConstraints() {
		applyConstraint("CREATE  CONSTRAINT ON (a:AwsVpc) ASSERT a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsEc2Instance) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsAsg) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsElb) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsRdsInstance) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsDeploymentGroup) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsKinesisStream) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsLaunchConfig) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsSnsTopic) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsSqsQueue) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsSubnet) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsAccount) assert a.aws_account IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsSnsSubscription) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsS3Bucket) assert a.aws_arn IS UNIQUE ");

		applyConstraint("CREATE  CONSTRAINT ON (a:AwsRoute53HostedZone) assert a.aws_arn IS UNIQUE ");
		
		applyConstraint("CREATE  CONSTRAINT ON (a:AwsRoute53RecordSet) assert a.id IS UNIQUE ");
	}

}
