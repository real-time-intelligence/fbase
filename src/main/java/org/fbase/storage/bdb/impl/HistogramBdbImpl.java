package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.column.HColumn;
import org.xerial.snappy.Snappy;

@Log4j2
public class HistogramBdbImpl extends QueryBdbApi implements HistogramDAO {

  private PrimaryIndex<ColumnKey, HColumn> primaryIndex;

  public HistogramBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(ColumnKey.class, HColumn.class);
  }

  @Override
  public void put(byte tableId, long blockId, int colId, int[][] data) {
    this.primaryIndex.put(new HColumn(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build(),
            CompressType.NONE, data, null, null));
  }

  @Override
  public void putCompressed(byte tableId, long blockId, int colId, int[][] data) {
    try {
      this.primaryIndex.put(new HColumn(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build(),
          CompressType.INT, null, Snappy.compress(data[0]), Snappy.compress(data[1])));
    } catch (IOException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int[][] get(byte tableId, long blockId, int colId) {
    HColumn hColumn = this.primaryIndex.get(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (isNotBlockCompressed(hColumn)) {
      return hColumn.getData();
    }

    try {
      int[] keys = Snappy.uncompressIntArray(hColumn.getKeysCompressed());

      int[][] data = new int[2][keys.length];
      data[0] = keys;
      data[1] = Snappy.uncompressIntArray(hColumn.getValuesCompressed());

      return data;
    } catch (Exception e) {
      log.catching(e);
    }

    return new int[0][0];
  }

}
