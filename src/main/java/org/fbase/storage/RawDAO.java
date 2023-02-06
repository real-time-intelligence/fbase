package org.fbase.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.fbase.model.profile.cstype.CType;

public interface RawDAO {

  void putKey(byte tableId, long key);

  void putByte(byte tableId, long key, int[] mapping, byte[][] data);

  void putInt(byte tableId, long key, int[] mapping, int[][] data);

  void putLong(byte tableId, long key, int[] mapping, long[][] data);

  void putFloat(byte tableId, long key, int[] mapping, float[][] data);

  void putDouble(byte tableId, long key, int[] mapping, double[][] data);

  void putString(byte tableId, long key, int[] mapping, String[][] data);

  void putEnum(byte tableId, long key, int[] mapping, byte[][] data);

  <T> void putCompressed(byte tableId, long key,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, List<List<String>> rawDataString)
      throws IOException;

  byte[] getRawByte(byte tableId, long key, int colIndex);

  int[] getRawInt(byte tableId, long key, int colIndex);

  float[] getRawFloat(byte tableId, long key, int colIndex);

  long[] getRawLong(byte tableId, long key, int colIndex);

  double[] getRawDouble(byte tableId, long key, int colIndex);

  String[] getRawString(byte tableId, long key, int colIndex);

  List<Long> getListKeys(byte tableId, long begin, long end);

  Map.Entry<Map.Entry<Long, Integer>, List<Object>> getColumnData(byte tableId, int colIndex, CType cType,
      boolean compression, int fetchSize, boolean isStarted,
      Entry<Long, Integer> pointer, AtomicInteger fetchCounter);

  long getPreviousKey(byte tableId, long begin);

  long getMaxKey(byte tableId);
}
