package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.raw.RColumn;
import org.fbase.storage.bdb.entity.raw.RMapping;
import org.fbase.storage.dto.RawDto;
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
    this.primaryIndex.putNoOverwrite(new RMapping(ColumnKey.builder().table(tableId).key(key).colIndex(0).build()));
  }

  @Override
  public void putByte(byte tableId, long key, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putInt(byte tableId, long key, int[] mapping, int[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataInt(data[i]).build());
    }
  }

  @Override
  public void putLong(byte tableId, long key, int[] mapping, long[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataLong(data[i]).build());
    }
  }

  @Override
  public void putFloat(byte tableId, long key, int[] mapping, float[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataFloat(data[i]).build());
    }
  }

  @Override
  public void putDouble(byte tableId, long key, int[] mapping, double[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataDouble(data[i]).build());
    }
  }

  @Override
  public void putString(byte tableId, long key, int[] mapping, String[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataString(data[i]).build());
    }
  }

  @Override
  public void putEnum(byte tableId, long key, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public <T> void putCompressed(byte tableId, long key,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, List<List<String>> rawDataString)
      throws IOException {

    for (int i = 0; i < rawDataLongMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(rawDataLongMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataDoubleMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(rawDataDoubleMapping.get(i)).build())
              .compressionType(CompressType.DOUBLE)
              .dataByte(Snappy.compress(rawDataDouble.get(i).stream().mapToDouble(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataStringMapping.size(); i++) {
      int[] lengthArray = rawDataString.get(i).stream()
              .mapToInt(s -> (int) s.codePoints().count())
              .toArray();

      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(ColumnKey.builder().table(tableId).key(key).colIndex(rawDataStringMapping.get(i)).build())
              .compressionType(CompressType.STRING)
              .dataInt(lengthArray)
              .dataByte(Snappy.compress(String.join("", rawDataString.get(i)))).build());
    }
  }

  public String[] getStringFromList(List<String> list) {
    String[] stringArray = new String[list.size()];
    int index = 0;
    for (String b : list) {
      stringArray[index++] = b;
    }
    return stringArray;
  }

  @Override
  public byte[] getRawByte(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataByte();
  }

  @Override
  public int[] getRawInt(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataInt();
  }

  @Override
  public long[] getRawLong(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataLong();
  }

  @Override
  public float[] getRawFloat(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataFloat();
  }

  @Override
  public double[] getRawDouble(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataDouble();
  }

  @Override
  public String[] getRawString(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataString();
  }

  @Override
  public RawDto getRawData(byte tableId, long key, int colIndex) {
    RColumn rColumn =
        this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    return RawDto.builder()
        .key(rColumn.getColumnKey().getKey())
        .dataByte(rColumn.getDataByte())
        .dataInt(rColumn.getDataInt())
        .dataLong(rColumn.getDataLong())
        .dataFloat(rColumn.getDataFloat())
        .dataDouble(rColumn.getDataDouble())
        .dataString(rColumn.getDataString()).build();
  }

  @Override
  public RawDto getCompressRawData(byte tableId, long key, int colIndex) throws IOException {
    RColumn rColumn =
        this.primaryIndexDataColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build());

    if (CompressType.LONG.equals(rColumn.getCompressionType())) {
      return RawDto.builder()
          .key(rColumn.getColumnKey().getKey())
          .dataLong(Snappy.uncompressLongArray(rColumn.getDataByte()))
          .build();
    } else if (CompressType.DOUBLE.equals(rColumn.getCompressionType())) {
      return RawDto.builder()
          .key(rColumn.getColumnKey().getKey())
          .dataDouble(Snappy.uncompressDoubleArray(rColumn.getDataByte()))
          .build();
    } else if (CompressType.STRING.equals(rColumn.getCompressionType())) {
      int[] length = rColumn.getDataInt();
      String uncompressString = Snappy.uncompressString(rColumn.getDataByte());
      List<String> dataString = new ArrayList<>();
      AtomicLong counter = new AtomicLong(0);
      Arrays.stream(length)
          .asLongStream()
          .forEach(l -> dataString.add(uncompressString.substring((int) counter.get(), (int) (counter.addAndGet(l)))));

      return RawDto.builder()
          .key(rColumn.getColumnKey().getKey())
          .dataString(getStringFromList(dataString))
          .build();
    }

    return RawDto.builder()
        .key(rColumn.getColumnKey().getKey()).build();
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
  public List<RColumn> getListRColumn(byte tableId) {
    List<RColumn> rColumns = new ArrayList<>();
    EntityCursor<RColumn> cursor = this.primaryIndexDataColumn.entities();

    if (cursor != null) {
      for (RColumn rColumn : cursor) {
        if (rColumn.getColumnKey().getTable() == tableId) {
          rColumns.add(rColumn);
        }
      }
      cursor.close();
    }

    return rColumns;
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
