package org.fbase.storage;

public interface HistogramDAO {

  void put(byte tableId, long blockId, int colId, int[][] data);

  void putCompressed(byte tableId, long blockId, int colId, int[][] data);

  void putCompressedKeysValues(byte tableId, long blockId, int colId, int[] keys, int[] values);

  int[][] get(byte tableId, long blockId, int colId);
}
