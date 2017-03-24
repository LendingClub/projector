package org.lendingclub.mercator.newrelic;

import org.junit.Test;
import org.lendingclub.mercator.core.BasicProjector;

public class NewRelicScannerTest {
	
	
	@Test
	public void test() {
		
		String accountId = "2345";
		String token = "asdfasdfx";
		
		NewRelicScanner scanner = new BasicProjector().createBuilder(NewRelicScannerBuilder.class).withAccountId(accountId).withToken(token).build();
		scanner.scan();	
	}

}
