package org.fbase.storage;

import java.io.IOException;
import java.util.List;
import org.fbase.storage.bdb.entity.raw.RColumn;
import org.fbase.storage.dto.RawDto;

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

  RawDto getRawData(byte tableId, long key, int colIndex);

  RawDto getCompressRawData(byte tableId, long key, int colIndex) throws IOException;

  List<Long> getListKeys(byte tableId, long begin, long end);

  List<RColumn> getListRColumn(byte tableId);

  long getPreviousKey(byte tableId, long begin);

  long getMaxKey(byte tableId);
}
