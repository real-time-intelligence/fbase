package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.Histogram;

@Log4j2
public class HistogramBdbImpl implements HistogramDAO {

  private PrimaryIndex<ColumnKey, Histogram> primaryIndex;

  public HistogramBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(ColumnKey.class, Histogram.class);
  }

  @Override
  public void put(byte tableId, long key, int colId, int[][] data) {
    this.primaryIndex.put(new Histogram(ColumnKey.builder().table(tableId).key(key).colIndex(colId).build(),
            CompressType.NONE, data));
  }

  @Override
  public int[][] get(byte tableId, long key, int colId) {
      return this.primaryIndex.get(ColumnKey.builder().table(tableId).key(key).colIndex(colId).build()).getData();
  }

}
