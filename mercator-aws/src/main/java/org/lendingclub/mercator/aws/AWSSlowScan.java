package org.lendingclub.mercator.aws;

/**
 * A marker interface to indicate a scan is either slow, or slow-changing and
 * should be scanned at a reduced rate.
 */
public interface AWSSlowScan {

	/**
	 * Returns the ratio (fraction) of the number of times this scanner should run
	 * compared to a normal scan. For example, the default value of 1/5 means the
	 * scanner should run once every 5 minutes (if a normal scan runs every 1
	 * minute.)
	 * 
	 * @See {@link MultiAccountRegionEntityScanner}
	 */
	public default int[] getSlowScanRatio() {
		return new int[] { 1, 5 };
	}

}
