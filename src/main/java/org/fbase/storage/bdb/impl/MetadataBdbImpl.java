package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.dto.GanttDto;
import org.fbase.storage.dto.MetadataDto;

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
  public MetadataDto getMetadata(byte table, long key) {
    ColumnKey columnKey = ColumnKey.builder().table(table).key(key).colIndex(0).build();
    Metadata md = this.primaryIndex.get(columnKey);
    return MetadataDto.builder().key(key)
        .dataType(md.getDataType())
        .storageType(md.getStorageType())
        .histograms(md.getHistograms())
        .build();
  }

  @Override
  public List<MetadataDto> getListMetadata(byte table, long begin, long end) {
    List<MetadataDto> list = new ArrayList<>();

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(begin).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(end).colIndex(0).build();

    EntityCursor<Metadata> cursor = doRangeQuery(this.primaryIndex, columnKeyBegin, true, columnKeyEnd, true);

    try (cursor) {
      for (Metadata md : cursor) {
        list.add(MetadataDto.builder()
            .key(md.getKey().getKey())
            .dataType(md.getDataType())
            .storageType(md.getStorageType())
            .histograms(md.getHistograms())
            .build());
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return list;
  }


  @Override
  public List<GanttDto> getListGanttDto(byte table, long begin, long end, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy) {

    List<GanttDto> list = new ArrayList<>();

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(begin).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(end).colIndex(0).build();

    EntityCursor<Metadata> cursor = doRangeQuery(this.primaryIndex, columnKeyBegin, true, columnKeyEnd, true);

    try (cursor) {
      for (Metadata md : cursor) {
        list.add(GanttDto.builder()
            .key(md.getKey().getKey())
            .firstHKey(md.getHistograms()[firstLevelGroupBy.getColId()])
            .secondHKey(md.getHistograms()[secondLevelGroupBy.getColId()])
            .build());
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return list;
  }

  @Override
  public long getPreviousKey(byte table, long begin) {
    long outPrev = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(0L).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(begin).colIndex(0).build();

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

  @Override
  public long getLastTimestamp(byte table, long begin, long end) {
    long lastTimestamp = 0L;

    ColumnKey columnKeyBegin = ColumnKey.builder().table(table).key(begin).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(table).key(begin).colIndex(0).build();

    EntityCursor<ColumnKey> cursor
        = this.primaryIndex.keys(columnKeyBegin, true, columnKeyEnd, true);

    if (cursor != null) {
      try (cursor) {
        if (cursor.last() != null) {
          lastTimestamp = cursor.last().getKey();
        }
      }
    }

    return lastTimestamp;
  }

}
