package org.lendingclub.projector.aws;

import javax.management.RuntimeErrorException;

import org.junit.Ignore;
import org.junit.Test;
import org.lendingclub.projector.core.BasicProjector;

import com.amazonaws.regions.Regions;

public class Demo {

	@Test
	@Ignore
	public void demo() {
		BasicProjector projector = new BasicProjector();
		
		new AWSScannerBuilder().withProjector(projector).withRegion(Regions.US_EAST_1).build(AllEntityScanner.class).scan();
		new AWSScannerBuilder().withProjector(projector).withRegion(Regions.US_EAST_2).build(AllEntityScanner.class).scan();
		new AWSScannerBuilder().withProjector(projector).withRegion(Regions.US_WEST_1).build(AllEntityScanner.class).scan();
		new AWSScannerBuilder().withProjector(projector).withRegion(Regions.US_WEST_2).build(AllEntityScanner.class).scan();
	}

}
