package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.HColumn;
import org.fbase.storage.bdb.entity.column.RColumn;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.util.CachedLastLinkedHashMap;
import org.xerial.snappy.Snappy;

@Log4j2
public class RawBdbImpl extends QueryBdbApi implements RawDAO {

  private final PrimaryIndex<MetadataKey, Metadata> primaryIndex;
  private final PrimaryIndex<ColumnKey, RColumn> primaryIndexDataColumn;

  public RawBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(MetadataKey.class, Metadata.class);
    this.primaryIndexDataColumn = store.getPrimaryIndex(ColumnKey.class, RColumn.class);
  }

  @Override
  public void putMetadata(byte tableId, long blockId, byte[] rawCTypeKeys, int[] rawColIds,
      int[] enumColIds, int[] histogramColIds) {
    this.primaryIndex.putNoOverwrite(
        new Metadata(MetadataKey.builder()
            .tableId(tableId)
            .blockId(blockId)
            .build(), rawCTypeKeys, rawColIds, enumColIds, histogramColIds)
    );
  }

  @Override
  public void putByte(byte tableId, long blockId, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putInt(byte tableId, long blockId, int[] mapping, int[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataInt(data[i]).build());
    }
  }

  @Override
  public void putLong(byte tableId, long blockId, int[] mapping, long[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataLong(data[i]).build());
    }
  }

  @Override
  public void putFloat(byte tableId, long blockId, int[] mapping, float[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataFloat(data[i]).build());
    }
  }

  @Override
  public void putDouble(byte tableId, long blockId, int[] mapping, double[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataDouble(data[i]).build());
    }
  }

  @Override
  public void putString(byte tableId, long blockId, int[] mapping, String[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataString(data[i]).build());
    }
  }

  @Override
  public void putEnum(byte tableId, long blockId, int[] mapping, byte[][] data) {
    for (int i = 0; i < mapping.length; i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder()
              .columnKey(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(mapping[i]).build())
              .dataByte(data[i]).build());
    }
  }

  @Override
  public void putCompressed(byte tableId, long blockId,
      List<Integer> rawDataTimeStampMapping, List<List<Long>> rawDataTimestamp,
      List<Integer> rawDataIntMapping, List<List<Integer>> rawDataInt,
      List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      List<Integer> rawDataFloatMapping, List<List<Float>> rawDataFloat,
      List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      List<Integer> rawDataStringMapping, List<List<String>> rawDataString)
      throws IOException {

    for (int i = 0; i < rawDataTimeStampMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataTimeStampMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataIntMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataIntMapping.get(i)).build())
              .compressionType(CompressType.INT)
              .dataByte(Snappy.compress(rawDataInt.get(i).stream().mapToInt(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataLongMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataLongMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray())).build());
    }

    for (int i = 0; i < rawDataFloatMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataFloatMapping.get(i)).build())
              .compressionType(CompressType.FLOAT)
              .dataByte(Snappy.compress(rawDataFloat.get(i).stream().mapToDouble(j -> j).toArray()))
              .build());
    }

    for (int i = 0; i < rawDataDoubleMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataDoubleMapping.get(i)).build())
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
                  ColumnKey.builder().tableId(tableId).blockId(blockId).colId(rawDataStringMapping.get(i)).build())
              .compressionType(CompressType.STRING)
              .dataInt(lengthArray)
              .dataByte(Snappy.compress(String.join("", rawDataString.get(i))))
              .build());
    }
  }

  @Override
  public void putCompressed(byte tableId, long blockId,
      CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping, List<List<Long>> rawDataTimestamp,
      CachedLastLinkedHashMap<Integer, Integer> rawDataIntMapping, List<List<Integer>> rawDataInt,
      CachedLastLinkedHashMap<Integer, Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      CachedLastLinkedHashMap<Integer, Integer> rawDataFloatMapping, List<List<Float>> rawDataFloat,
      CachedLastLinkedHashMap<Integer, Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      CachedLastLinkedHashMap<Integer, Integer> rawDataStringMapping, List<List<String>> rawDataString) {

    rawDataTimeStampMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.LONG)
                .dataByte(Snappy.compress(rawDataTimestamp.get(i).stream().mapToLong(j -> j).toArray())).build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataIntMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.INT)
                .dataByte(Snappy.compress(rawDataInt.get(i).stream().mapToInt(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataLongMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.LONG)
                .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataFloatMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.FLOAT)
                .dataByte(Snappy.compress(rawDataFloat.get(i).stream().mapToDouble(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataDoubleMapping.forEach((colId, i) -> {
      try {
        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.DOUBLE)
                .dataByte(Snappy.compress(rawDataDouble.get(i).stream().mapToDouble(j -> j).toArray()))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    rawDataStringMapping.forEach((colId, i) -> {
      try {
        int[] lengthArray = rawDataString.get(i).stream()
            .mapToInt(s -> (int) s.codePoints().count())
            .toArray();

        this.primaryIndexDataColumn.putNoOverwrite(
            RColumn.builder().columnKey(
                    ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build())
                .compressionType(CompressType.STRING)
                .dataInt(lengthArray)
                .dataByte(Snappy.compress(String.join("", rawDataString.get(i))))
                .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public byte[] getRawByte(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for byte t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new byte[0];
    }

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
  public int[] getRawInt(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for int t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new int[0];
    }

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
  public long[] getRawLong(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for long t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new long[0];
    }

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
  public float[] getRawFloat(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for float t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new float[0];
    }

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
  public double[] getRawDouble(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for double t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new double[0];
    }

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
  public String[] getRawString(byte tableId, long blockId, int colId) {
    RColumn rColumn = this.primaryIndexDataColumn.get(
        ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (rColumn == null) {
      log.info("No data found for string t::b::c -> " + tableId + "::" + blockId + "::" + colId);
      return new String[0];
    }

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
  public List<Long> getListBlockIds(byte tableId, long begin, long end) {
    List<Long> list = new ArrayList<>();

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();
    EntityCursor<Metadata> cursor = doRangeQuery(this.primaryIndex, beginMK, true, endMK, true);

    try (cursor) {
      for (Metadata metadata : cursor) {
        list.add(metadata.getMetadataKey().getBlockId());
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return list;
  }

  @Override
  public EntityCursor<Metadata> getMetadataEntityCursor(MetadataKey begin, MetadataKey end) {
    return doRangeQuery(this.primaryIndex, begin, true, end, true);
  }

  @Override
  public Metadata getMetadata(MetadataKey metadataKey) {
    Metadata metadata = this.primaryIndex.get(metadataKey);

    if (metadata == null) {
      log.info("No data found for metadata key -> " + metadataKey);
      return new Metadata();
    }

    return metadata;
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
  public long getPreviousBlockId(byte tableId, long blockId) {
    return getLastBlockIdLocal(tableId, 0L, blockId);
  }

  @Override
  public long getLastBlockId(byte tableId) {
    return getLastBlockIdLocal(tableId, 0L, Long.MAX_VALUE);
  }

  @Override
  public long getLastBlockId(byte tableId, long begin, long end) {
    return getLastBlockIdLocal(tableId, begin, end);
  }

  private long getLastBlockIdLocal(byte tableId, long begin, long end) {
    long lastBlockId = 0L;

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(end).build();

    EntityCursor<MetadataKey> cursor
        = this.primaryIndex.keys(beginMK, true, endMK, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          lastBlockId = cursor.last().getBlockId();
        }
      }
    }

    return lastBlockId;
  }

}
