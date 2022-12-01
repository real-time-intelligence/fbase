package org.fbase.storage;

public interface HistogramDAO {

  int put(int[][] data);

  int[][] get(int key);
}
