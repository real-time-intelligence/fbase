package org.fbase.storage.bdb.impl;

import static org.fbase.service.mapping.Mapper.DOUBLE_NULL;
import static org.fbase.service.mapping.Mapper.FLOAT_NULL;
import static org.fbase.service.mapping.Mapper.INT_NULL;
import static org.fbase.service.mapping.Mapper.LONG_NULL;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.raw.RColumn;
import org.fbase.storage.bdb.entity.raw.RMapping;
import org.xerial.snappy.Snappy;

@Log4j2
public class RawBdbImpl extends QueryBdbApi implements RawDAO {

  private PrimaryIndex<ColumnKey, RMapping> primaryIndex;
  private PrimaryIndex<ColumnKey, RColumn> primaryIndexDataColumn;

  public RawBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(ColumnKey.class, RMapping.class);
    this.primaryIndexDataColumn = store.getPrimaryIndex(ColumnKey.class, RColumn.class);
  }

  @Override
  public void putKey(byte tableId, long key) {
    this.primaryIndex.putNoOverwrite(
        new RMapping(ColumnKey.builder().table(tableId).key(key).colIndex(0).build()));
  }

  @Override
  public void putByte(byte tableId, long key, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putInt(byte tableId, long key, int[] mapping, int[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataInt(data[i]).build());
    }
  }

  @Override
  public void putLong(byte tableId, long key, int[] mapping, long[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataLong(data[i]).build());
    }
  }

  @Override
  public void putFloat(byte tableId, long key, int[] mapping, float[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataFloat(data[i]).build());
    }
  }

  @Override
  public void putDouble(byte tableId, long key, int[] mapping, double[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataDouble(data[i]).build());
    }
  }

  @Override
  public void putString(byte tableId, long key, int[] mapping, String[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataString(data[i]).build());
    }
  }

  @Override
  public void putEnum(byte tableId, long key, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putCompressed(byte tableId, long key,
      List<Integer> rawDataTimeStampMapping, List<List<Long>> rawDataTimestamp,
      List<Integer> rawDataIntMapping, List<List<Integer>> rawDataInt,
      List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      List<Integer> rawDataFloatMapping, List<List<Float>> rawDataFloat,
      List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      List<Integer> rawDataStringMapping, List<List<String>> rawDataString,
      List<Integer> rawDataEnumMapping, List<List<Byte>> rawDataEnum)
      throws IOException {

    for (int i = 0; i < rawDataTimeStampMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataTimeStampMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataIntMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataIntMapping.get(i)).build())
              .compressionType(CompressType.INT)
              .dataByte(Snappy.compress(rawDataInt.get(i).stream().mapToInt(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataLongMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataLongMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataFloatMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataFloatMapping.get(i)).build())
              .compressionType(CompressType.FLOAT)
              .dataByte(Snappy.compress(rawDataFloat.get(i).stream().mapToDouble(j -> j).toArray()))
              .build());
    }

    for (int i = 0; i < rawDataDoubleMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataDoubleMapping.get(i)).build())
              .compressionType(CompressType.DOUBLE)
              .dataByte(Snappy.compress(rawDataDouble.get(i).stream().mapToDouble(j -> j).toArray()))
              .build());
    }

    for (int i = 0; i < rawDataStringMapping.size(); i++) {
      int[] lengthArray = rawDataString.get(i).stream()
          .mapToInt(s -> (int) s.codePoints().count())
          .toArray();

      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataStringMapping.get(i)).build())
              .compressionType(CompressType.STRING)
              .dataInt(lengthArray)
              .dataByte(Snappy.compress(String.join("", rawDataString.get(i)))).build());
    }

    for (int i = 0; i < rawDataEnumMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataEnumMapping.get(i)).build())
              .compressionType(CompressType.BYTE)
              .dataByte(Snappy.compress(getByteFromList(rawDataEnum.get(i))))
              .build());
    }
  }

  @Override
  public byte[] getRawByte(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataByte();
    }

    try {
      return Snappy.uncompress(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new byte[0];
  }

  @Override
  public int[] getRawInt(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataInt();
    }

    try {
      return Snappy.uncompressIntArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new int[0];
  }

  @Override
  public long[] getRawLong(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataLong();
    }

    try {
      return Snappy.uncompressLongArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new long[0];
  }

  @Override
  public float[] getRawFloat(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataFloat();
    }

    try {
      return convertDoubleArrayToFloatArray(Snappy.uncompressDoubleArray(rColumn.getDataByte()));
    } catch (Exception e) {
      log.catching(e);
    }

    return new float[0];
  }

  @Override
  public double[] getRawDouble(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataDouble();
    }

    try {
      return Snappy.uncompressDoubleArray(rColumn.getDataByte());
    } catch (Exception e) {
      log.catching(e);
    }

    return new double[0];
  }

  @Override
  public String[] getRawString(byte tableId, long key, int colIndex) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (isNotBlockCompressed(rColumn)) {
      return rColumn.getDataString();
    }

    try {
      String uncompressString = Snappy.uncompressString(rColumn.getDataByte());
      List<String> dataString = new ArrayList<>();
      AtomicLong counter = new AtomicLong(0);
      Arrays.stream(rColumn.getDataInt())
          .asLongStream()
          .forEach(l ->
              dataString.add(uncompressString.substring((int) counter.get(), (int) (counter.addAndGet(l)))));

      return getStringFromList(dataString);
    } catch (Exception e) {
      log.catching(e);
    }

    return new String[0];
  }

  @Override
  public List<Long> getListKeys(byte tableId, long begin, long end) {
    List<Long> list = new ArrayList<>();

    ColumnKey columnKeyBegin = ColumnKey.builder().table(tableId).key(begin).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(tableId).key(end).colIndex(0).build();
    EntityCursor<RMapping> cursor = doRangeQuery(this.primaryIndex, columnKeyBegin, true, columnKeyEnd, true);

    try (cursor) {
      for (RMapping rm : cursor) {
        list.add(rm.getKey().getKey());
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return list;
  }

  @Override
  public Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId, int colIndex, int tsColIndex,
      CType cType, int fetchSize, boolean isStarted, long maxKey, Entry<Long, Integer> pointer, AtomicInteger fetchCounter) {

    List<Object> columnData = new ArrayList<>();

    boolean isPointerFirst = true;
    boolean getNextPointer = false;

    if (tsColIndex != -1) {
      long prevKey = this.getPreviousKey(tableId, pointer.getKey());
      if (prevKey != pointer.getKey() & prevKey != 0) {
        isStarted = false;
        RColumn rColumn =
            this.primaryIndexDataColumn.get(
                ColumnKey.builder().table(tableId).key(prevKey).colIndex(tsColIndex).build());

        if (rColumn.getCompressionType() == null || CompressType.NONE.equals(rColumn.getCompressionType())) {
          int length = getLengthByColumn(rColumn, CType.LONG);

          for (int i = 0; i < length; i++) {
            if (rColumn.getDataLong()[i] == pointer.getKey()) {
              pointer = Map.entry(prevKey, i);
            }
          }
        } else {
          long[] uncompressed;
          try {
            uncompressed = Snappy.uncompressLongArray(rColumn.getDataByte());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          int lengthByColumn = uncompressed.length;

          for (int i = 0; i < lengthByColumn; i++) {
            if (uncompressed[i] == pointer.getKey()) {
              pointer = Map.entry(prevKey, i);
            }
          }
        }
      }
    }

    ColumnKey columnKeyBegin = ColumnKey.builder().table(tableId).key(pointer.getKey()).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(tableId).key(maxKey).colIndex(0).build();
    EntityCursor<RMapping> cursor = doRangeQuery(this.primaryIndex, columnKeyBegin, true, columnKeyEnd, true);

    try (cursor) {
      RMapping columnKey;

      while ((columnKey = cursor.next()) != null) {
        RColumn rColumn =
            this.primaryIndexDataColumn.get(
                ColumnKey.builder().table(tableId).key(columnKey.getKey().getKey()).colIndex(colIndex).build());

        if (getNextPointer) {
          return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), 0), columnData);
        }

        // Not compressed
        if (rColumn.getCompressionType() == null || CompressType.NONE.equals(rColumn.getCompressionType())) {
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;
          int length = getLengthByColumn(rColumn, cType);

          for (int i = startPoint; i < length; i++) {
            columnData.add(getStrValueForCell(rColumn, cType, i));
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        } else {
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          if (CompressType.INT.equals(rColumn.getCompressionType())) {
            try {
              int[] uncompressed = Snappy.uncompressIntArray(rColumn.getDataByte());
              int lengthByColumn = uncompressed.length;

              for (int i = startPoint; i < lengthByColumn; i++) {
                columnData.add(String.valueOf(uncompressed[i] == INT_NULL ? "" : uncompressed[i]));

                fetchCounter.decrementAndGet();
                if (fetchCounter.get() == 0) {
                  if (i == lengthByColumn - 1) {
                    getNextPointer = true;
                  } else {
                    return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), i + 1), columnData);
                  }
                }
              }
            } catch (IOException e) {
              log.error(e);
            }
          } else if (CompressType.LONG.equals(rColumn.getCompressionType())) {
              try {
                long[] uncompressed = Snappy.uncompressLongArray(rColumn.getDataByte());
                int lengthByColumn = uncompressed.length;

                for (int i = startPoint; i < lengthByColumn; i++) {
                  columnData.add(String.valueOf(uncompressed[i] == LONG_NULL ? "" : uncompressed[i]));

                  fetchCounter.decrementAndGet();
                  if (fetchCounter.get() == 0) {
                    if (i == lengthByColumn - 1) {
                      getNextPointer = true;
                    } else {
                      return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), i + 1), columnData);
                    }
                  }
                }
              } catch (IOException e) {
                log.error(e);
              }
            } else if (CompressType.DOUBLE.equals(rColumn.getCompressionType())) {
              try {
                double[] uncompressed = Snappy.uncompressDoubleArray(rColumn.getDataByte());
                int lengthByColumn = uncompressed.length;

                for (int i = startPoint; i < lengthByColumn; i++) {
                  columnData.add(String.valueOf(uncompressed[i] == DOUBLE_NULL ? "" : uncompressed[i]));
                  fetchCounter.decrementAndGet();
                  if (fetchCounter.get() == 0) {
                    if (i == lengthByColumn - 1) {
                      getNextPointer = true;
                    } else {
                      return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), i + 1), columnData);
                    }
                  }
                }
              } catch (IOException e) {
                log.error(e);
              }
            } else if (CompressType.STRING.equals(rColumn.getCompressionType())) {
              try {
                int[] length = rColumn.getDataInt();
                String uncompressString = Snappy.uncompressString(rColumn.getDataByte());
                List<String> dataString = new ArrayList<>();
                AtomicLong counter = new AtomicLong(0);
                Arrays.stream(length)
                    .asLongStream()
                    .forEach(l -> dataString.add(
                        uncompressString.substring((int) counter.get(), (int) (counter.addAndGet(l)))));

                String[] uncompressed = getStringFromList(dataString);
                int lengthByColumn = uncompressed.length;

                for (int i = startPoint; i < lengthByColumn; i++) {
                  columnData.add(uncompressed[i] == null ? "" : uncompressed[i]);
                  fetchCounter.decrementAndGet();
                  if (fetchCounter.get() == 0) {
                    if (i == lengthByColumn - 1) {
                      getNextPointer = true;
                    } else {
                      return Map.entry(Map.entry(rColumn.getColumnKey().getKey(), i + 1), columnData);
                    }
                  }
                }
              } catch (IOException e) {
                log.error(e);
              }
            }

            if (isPointerFirst) isPointerFirst = false;
          }

      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    cursor.close();

    return Map.entry(Map.entry(maxKey + 1, 0), columnData);
  }

  public String[] getStringFromList(List<String> list) {
    String[] stringArray = new String[list.size()];
    int index = 0;
    for (String b : list) {
      stringArray[index++] = b;
    }
    return stringArray;
  }

  public String getStrValueForCell(RColumn rColumn, CType cType, int iRow) {
    if (CType.INT == cType) {
      return String.valueOf(rColumn.getDataInt()[iRow] == INT_NULL ? "" : rColumn.getDataInt()[iRow]);
    } else if (CType.LONG == cType) {
      return String.valueOf(rColumn.getDataLong()[iRow] == LONG_NULL ? "" : rColumn.getDataLong()[iRow]);
    } else if (CType.FLOAT == cType) {
      return String.valueOf(rColumn.getDataFloat()[iRow] == FLOAT_NULL ? "" : rColumn.getDataFloat()[iRow]);
    } else if (CType.DOUBLE == cType) {
      return String.valueOf(rColumn.getDataDouble()[iRow] == DOUBLE_NULL ? "" : rColumn.getDataDouble()[iRow]);
    } else if (CType.STRING == cType) {
      return rColumn.getDataString()[iRow] == null ? "" : rColumn.getDataString()[iRow];
    } else {
      return "";
    }
  }

  private int getLengthByColumn(RColumn rColumn, CType cType) {
    if (CType.INT == cType) {
      return rColumn.getDataInt().length;
    } else if (CType.LONG == cType) {
      return rColumn.getDataLong().length;
    } else if (CType.DOUBLE == cType) {
      return rColumn.getDataDouble().length;
    } else if (CType.STRING == cType) {
      return rColumn.getDataString().length;
    }

    return rColumn.getDataString().length;
  }

  @Override
  public long getPreviousKey(byte tableId, long begin) {
    return getMaxValue(tableId, 0L, begin);
  }

  @Override
  public long getMaxKey(byte tableId) {
    return getMaxValue(tableId, 0L, Long.MAX_VALUE);
  }

  private long getMaxValue(byte tableId, long beginKey, long endKey) {
    long maxKey = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(tableId).key(beginKey).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(tableId).key(endKey).colIndex(0).build();

    EntityCursor<ColumnKey> cursor
        = this.primaryIndex.keys(columnKeyBegin, true, columnKeyEnd, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          maxKey = cursor.last().getKey();
        }
      }
    }

    return maxKey;
  }
}
