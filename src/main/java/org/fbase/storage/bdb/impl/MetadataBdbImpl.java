package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.Metadata;

@Log4j2
public class MetadataBdbImpl extends QueryBdbApi implements MetadataDAO {
  private PrimaryIndex<ColumnKey, Metadata> primaryIndex;

  public MetadataBdbImpl(EntityStore store) {
    this.primaryIndex = store.getPrimaryIndex(ColumnKey.class, Metadata.class);
  }

  @Override
  public boolean put(byte table, long key, byte[] datatype, byte[] function, int[] histograms) {
    ColumnKey columnKey = ColumnKey.builder().table(table).key(key).colIndex(0).build();
    return this.primaryIndex.putNoOverwrite(new Metadata(columnKey, datatype, function, histograms));
  }

  @Override
  public int[] getHistograms(byte table, long key) {
    ColumnKey columnKey = ColumnKey.builder().table(table).key(key).colIndex(0).build();
    return this.primaryIndex.get(columnKey).getHistograms();
  }

  @Override
  public long getPreviousKey(byte table, long begin) {
    long prevKey = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(0L).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(begin).colIndex(0).build();

    EntityCursor<ColumnKey> cursor
        = this.primaryIndex.keys(columnKeyBegin, true, columnKeyEnd, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          prevKey = cursor.last().getKey();
        }
      }
    }

    return prevKey;
  }

  @Override
  public long getLastTimestamp(byte table, long begin, long end) {
    long lastTs = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(begin).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(begin).colIndex(0).build();

    EntityCursor<ColumnKey> cursor
        = this.primaryIndex.keys(columnKeyBegin, true, columnKeyEnd, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          lastTs = cursor.last().getKey();
        }
      }
    }

    return lastTs;
  }

}
