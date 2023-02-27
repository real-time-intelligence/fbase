package org.fbase.storage;

public interface HistogramDAO {

  void put(byte tableId, long key, int colId, int[][] data);

  int[][] get(byte tableId, long key, int colId);
}
