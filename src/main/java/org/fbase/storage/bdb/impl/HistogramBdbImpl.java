package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.bdb.entity.Histogram;

@Log4j2
public class HistogramBdbImpl implements HistogramDAO {

  private PrimaryIndex<Integer, Histogram> primaryIndex;

  public HistogramBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(Integer.class, Histogram.class);
  }

  @Override
  public int put(int[][] data) {
    int maxKey = getCurrentValue() + 1;
    this.primaryIndex.put(new Histogram(maxKey, data));
    return maxKey;
  }

  @Override
  public int[][] get(int key) {
    if (this.primaryIndex.contains(key))
      return this.primaryIndex.get(key).getData();

    return new int[0][0];
  }

  private int getCurrentValue() {
    int out = 0;

    EntityCursor<Integer> cursor
        = this.primaryIndex.keys(0, true, Integer.MAX_VALUE, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          out = cursor.last();
        }
      }
    }

    return out;
  }
}
