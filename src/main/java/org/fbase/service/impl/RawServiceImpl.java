package org.fbase.service.impl;

import com.sleepycat.persist.EntityCursor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.MetaModel;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.RawService;
import org.fbase.service.mapping.Mapper;
import org.fbase.sql.BatchResultSet;
import org.fbase.sql.BatchResultSetImpl;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.raw.RMapping;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class RawServiceImpl extends CommonServiceApi implements RawService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final HistogramDAO histogramDAO;
  private final EnumDAO enumDAO;

  public RawServiceImpl(MetaModel metaModel, Converter converter, RawDAO rawDAO, HistogramDAO histogramDAO, EnumDAO enumDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.histogramDAO = histogramDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException {
    byte tableId = getTableId(tableName, metaModel);
    CProfile tsProfile = getTimestampProfile(getCProfiles(tableName, metaModel));

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long prevKey = this.rawDAO.getPreviousKey(tableId, begin);

    if (prevKey != begin & prevKey != 0) {
      this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, prevKey, begin, end, list);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, key, begin, end, list));

    return list;
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName, long begin, long end) {
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName, CProfile cProfile, long begin, long end) {
    CProfile tsProfile = getTsProfile(tableName);
    List<CProfile> cProfiles = List.of(tsProfile, cProfile);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName, long begin, long end, int fetchSize) {
    byte tableId = getTableId(tableName, metaModel);
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    return new BatchResultSetImpl(tableName, tableId, fetchSize, begin, end, cProfiles, this);
  }

  @Override
  public Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId, int colIndex, int tsColIndex,
      CProfile cProfile, int fetchSize, boolean isStarted, long maxKey, Entry<Long, Integer> pointer, AtomicInteger fetchCounter) {

    List<Object> columnData = new ArrayList<>();

    boolean isPointerFirst = true;
    boolean getNextPointer = false;

    if (tsColIndex != -1) {
      long prevKey = this.rawDAO.getPreviousKey(tableId, pointer.getKey());
      if (prevKey != pointer.getKey() & prevKey != 0) {
        isStarted = false;

        long[] timestamps = rawDAO.getRawLong(tableId, prevKey, tsColIndex);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] == pointer.getKey()) {
            pointer = Map.entry(prevKey, i);
          }
        }
      }
    }

    ColumnKey columnKeyBegin = ColumnKey.builder().table(tableId).key(pointer.getKey()).colIndex(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().table(tableId).key(maxKey).colIndex(0).build();
    EntityCursor<RMapping> cursor = rawDAO.getRMappingEntityCursor(columnKeyBegin, columnKeyEnd);

    try (cursor) {
      RMapping columnKey;

      while ((columnKey = cursor.next()) != null) {
        long key = columnKey.getKey().getKey();

        if (getNextPointer) {
          return Map.entry(Map.entry(key, 0), columnData);
        }

        if (cProfile.getCsType().isTimeStamp()) { // timestamp
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          long[] timestamps = rawDAO.getRawLong(tableId, key, cProfile.getColId());

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(timestamps[i]);
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(key, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.RAW & !cProfile.getCsType().isTimeStamp()) { // raw data
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          String[] column = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
              tableId, key, cProfile.getColId());

          for (int i = startPoint; i < column.length; i++) {
            columnData.add(column[i]);
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == column.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(key, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.HISTOGRAM) { // indexed data
          long[] timestamps = rawDAO.getRawLong(tableId, key, tsColIndex);

          int[][] h = histogramDAO.get(tableId, key, cProfile.getColId());

          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(key, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.ENUM) { // enum data
          long[] timestamps = rawDAO.getRawLong(tableId, key, tsColIndex);

          byte[] bytes = this.rawDAO.getRawByte(tableId, key, cProfile.getColId());

          int[] eColumn = enumDAO.getEColumnValues(tableId, key, cProfile.getColId());

          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn, bytes[i]), cProfile));
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(key, i + 1), columnData);
              }
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

  @Override
  public long getMaxKey(byte tableId) {
    return rawDAO.getMaxKey(tableId);
  }

  private List<List<Object>> getRawData(String tableName, List<CProfile> cProfiles, long begin, long end) {
    byte tableId = getTableId(tableName, metaModel);
    CProfile tsProfile = getTsProfile(tableName);

    List<List<Object>> columnDataListOut = new ArrayList<>();

    long prevKey = this.rawDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, prevKey, begin, end, columnDataListOut);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key ->
            this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, key, begin, end, columnDataListOut));

    return columnDataListOut;
  }

  private CProfile getTsProfile(String tableName) {
    return getCProfiles(tableName, metaModel).stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findFirst()
        .orElseThrow();
  }

  private void computeRawDataBeginEnd(byte tableId, CProfile tsProfile, List<CProfile> cProfiles,
      long key, long begin, long end, List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, key, tsProfile.getColId());

    cProfiles.forEach(cProfile -> {

      if (cProfile.getCsType().isTimeStamp()) { // timestamp
        List<Object> columnData = new ArrayList<>();

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            columnData.add(timestamps[i]);
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 0 : cProfile.getColId(), columnData);
      }

      List<Object> columnData = new ArrayList<>();

      if (cProfile.getCsType().getSType() == SType.RAW & !cProfile.getCsType().isTimeStamp()) { // raw data

        String[] column = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
            tableId, key, cProfile.getColId());

        IntStream iRow = IntStream.range(0, timestamps.length);
        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            columnData.add(column[iR]);
          }
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

      if (cProfile.getCsType().getSType() == SType.HISTOGRAM) { // indexed data
        int[][] h = histogramDAO.get(tableId, key, cProfile.getColId());

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

      if (cProfile.getCsType().getSType() == SType.ENUM) { // enum data

        byte[] bytes = this.rawDAO.getRawByte(tableId, key, cProfile.getColId());

        int[] eColumn = enumDAO.getEColumnValues(tableId, key, cProfile.getColId());

        IntStream iRow = IntStream.range(0, timestamps.length);

        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn, bytes[iR]), cProfile));
          }
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }

  private void computeNoIndexBeginEnd(byte tableId, CProfile tProfile, CProfile cProfile,
      long key, long begin, long end, List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, key, tProfile.getColId());

    String[] column = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
        tableId, key, cProfile.getColId());

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(column[iR], (k, val) -> val == null ? 1 : val + 1);
      }
    });

    list.add(StackedColumn.builder()
        .key(key)
        .tail(tail)
        .keyCount(map).build());
  }

}
