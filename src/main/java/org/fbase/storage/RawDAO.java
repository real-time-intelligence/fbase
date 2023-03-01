package org.fbase.storage;

import com.sleepycat.persist.EntityCursor;
import java.io.IOException;
import java.util.List;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.CMetadata;

public interface RawDAO {

  void putBlockId(byte tableId, long blockId);

  void putByte(byte tableId, long blockId, int[] mapping, byte[][] data);

  void putInt(byte tableId, long blockId, int[] mapping, int[][] data);

  void putLong(byte tableId, long blockId, int[] mapping, long[][] data);

  void putFloat(byte tableId, long blockId, int[] mapping, float[][] data);

  void putDouble(byte tableId, long blockId, int[] mapping, double[][] data);

  void putString(byte tableId, long blockId, int[] mapping, String[][] data);

  void putEnum(byte tableId, long blockId, int[] mapping, byte[][] data);

  void putCompressed(byte tableId, long blockId,
      List<Integer> rawDataTimeStampMapping, List<List<Long>> rawDataTimestamp,
      List<Integer> rawDataIntMapping, List<List<Integer>> rawDataInt,
      List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      List<Integer> rawDataFloatMapping, List<List<Float>> rawDataFloat,
      List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      List<Integer> rawDataStringMapping, List<List<String>> rawDataString,
      List<Integer> rawDataEnumMapping, List<List<Byte>> rawDataEnum)
      throws IOException;

  byte[] getRawByte(byte tableId, long blockId, int colId);

  int[] getRawInt(byte tableId, long blockId, int colId);

  float[] getRawFloat(byte tableId, long blockId, int colId);

  long[] getRawLong(byte tableId, long blockId, int colId);

  double[] getRawDouble(byte tableId, long blockId, int colId);

  String[] getRawString(byte tableId, long blockId, int colId);

  List<Long> getListBlockIds(byte tableId, long begin, long end);

  EntityCursor<CMetadata> getCMetadataEntityCursor(ColumnKey begin, ColumnKey end);

  long getPreviousBlockId(byte tableId, long blockId);

  long getLastBlockId(byte tableId);

  long getLastBlockId(byte tableId, long begin, long end);
}
