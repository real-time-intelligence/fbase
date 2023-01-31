package org.fbase.storage;

public interface MetadataDAO {

  boolean put(byte table, long key, byte[] datatype, byte[] function, int[] histograms);

  int[] getHistograms(byte table, long key);

  long getPreviousKey(byte table, long begin);

  long getLastTimestamp(byte table, long begin, long end);
}
