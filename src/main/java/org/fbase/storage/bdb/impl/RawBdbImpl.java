package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.raw.RColumn;
import org.fbase.storage.bdb.entity.raw.RMapping;
import org.fbase.storage.dto.RawDto;

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
    long outPrev = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(tableId).key(0L).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(tableId).key(begin).colIndex(0).build();

    EntityCursor<ColumnKey> cursor
        = this.primaryIndex.keys(columnKeyBegin, true, columnKeyEnd, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          outPrev = cursor.last().getKey();
        }
      }
    }

    return outPrev;
  }
}
