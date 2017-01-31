package org.lendingclub.projector.aws;

import org.junit.Test;

import com.amazonaws.regions.Regions;

public class AMIScannerIntegrationTest extends AWSIntegrationTest {

	@Test
	public void testIt() {


		
		AWSScannerBuilder b = new AWSScannerBuilder()
				.withProjector(getProjector()).withRegion(Regions.US_WEST_2);

		b.build(AllEntityScanner.class).scan();

	}
}
