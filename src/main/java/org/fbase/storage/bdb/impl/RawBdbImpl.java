package org.fbase.storage.bdb.impl;

import static org.fbase.core.Mapper.DOUBLE_NULL;
import static org.fbase.core.Mapper.FLOAT_NULL;
import static org.fbase.core.Mapper.INT_NULL;
import static org.fbase.core.Mapper.LONG_NULL;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
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
  public <T> void putCompressed(byte tableId, long key,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, List<List<String>> rawDataString)
      throws IOException {

    for (int i = 0; i < rawDataLongMapping.size(); i++) {
      this.primaryIndexDataColumn.putNoOverwrite(
          RColumn.builder().columnKey(
                  ColumnKey.builder().table(tableId).key(key).colIndex(rawDataLongMapping.get(i)).build())
              .compressionType(CompressType.LONG)
              .dataByte(Snappy.compress(rawDataLong.get(i).stream().mapToLong(j -> j).toArray())).build());
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
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataByte();
  }

  @Override
  public int[] getRawInt(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataInt();
  }

  @Override
  public long[] getRawLong(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataLong();
  }

  @Override
  public float[] getRawFloat(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataFloat();
  }

  @Override
  public double[] getRawDouble(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataDouble();
  }

  @Override
  public String[] getRawString(byte tableId, long key, int colIndex) {
    return this.primaryIndexDataColumn.get(
        ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getDataString();
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
  public List<Object> getColumnData(byte tableId, int colIndex, boolean compression, CType cType) {
    List<Object> columnData = new ArrayList<>();

    for (long i = 0; i <= getMaxKey(tableId); i++) {
      RColumn rColumn =
          this.primaryIndexDataColumn.get(
              ColumnKey.builder().table(tableId).key(i).colIndex(colIndex).build());

      if (compression) {
        if (CompressType.LONG.equals(rColumn.getCompressionType())) {
          try {
            long[] uncompressed = Snappy.uncompressLongArray(rColumn.getDataByte());
            IntStream iRow = IntStream.range(0, uncompressed.length);
            iRow.forEach(
                iR -> columnData.add(String.valueOf(uncompressed[iR] == LONG_NULL ? "" : uncompressed[iR])));
          } catch (IOException e) {
            log.error(e);
          }
        } else if (CompressType.DOUBLE.equals(rColumn.getCompressionType())) {
          try {
            double[] uncompressed = Snappy.uncompressDoubleArray(rColumn.getDataByte());
            IntStream iRow = IntStream.range(0, uncompressed.length);
            iRow.forEach(iR -> columnData.add(
                String.valueOf(uncompressed[iR] == DOUBLE_NULL ? "" : uncompressed[iR])));
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
            IntStream iRow = IntStream.range(0, uncompressed.length);
            iRow.forEach(iR -> columnData.add(uncompressed[iR] == null ? "" : uncompressed[iR]));
          } catch (IOException e) {
            log.error(e);
          }
        }
      } else {
        IntStream iRow = IntStream.range(0, getLengthByColumn(rColumn, cType));
        iRow.forEach(iR -> columnData.add(getStrValueForCell(rColumn, cType, iR)));
      }
    }

    return columnData;
  }

  public String getStrValueForCell(RColumn rColumn, CType cType, int iRow) {
    if (CType.INT == cType) {
      return String.valueOf(rColumn.getDataInt()[iRow] == INT_NULL ? "" : rColumn.getDataInt()[iRow]);
    } else if (CType.LONG == cType) {
      return String.valueOf(rColumn.getDataLong()[iRow] == LONG_NULL ? "" : rColumn.getDataLong()[iRow]);
    } else if (CType.FLOAT == cType) {
      return String.valueOf(rColumn.getDataFloat()[iRow] == FLOAT_NULL ? "" : rColumn.getDataFloat()[iRow]);
    } else if (CType.DOUBLE == cType) {
      return String.valueOf(
          rColumn.getDataDouble()[iRow] == DOUBLE_NULL ? "" : rColumn.getDataDouble()[iRow]);
    } else if (CType.STRING == cType) {
      return rColumn.getDataString()[iRow] == null ? "" : rColumn.getDataString()[iRow];
    } else {
      return "";
    }
  }

  private int getLengthByColumn(RColumn rColumn, CType cType) {
    if (CType.LONG == cType) {
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
