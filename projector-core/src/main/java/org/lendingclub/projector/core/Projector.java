package org.lendingclub.projector.core;

import java.util.Map;

import io.macgyver.neorx.rest.NeoRxClient;

public interface Projector {
	
	public Map<String,String> getProperties();
	public NeoRxClient getNeoRx();
	
}
