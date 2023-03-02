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
import org.fbase.storage.bdb.entity.CMetadata;
import org.fbase.storage.bdb.entity.column.EColumn;
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

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);

    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, previousBlockId, begin, end, list);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
            this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, blockId, begin, end, list));

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
  public Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId, int colId, int tsColId,
      CProfile cProfile, int fetchSize, boolean isStarted, long maxBlockId, Entry<Long, Integer> pointer, AtomicInteger fetchCounter) {

    List<Object> columnData = new ArrayList<>();

    boolean isPointerFirst = true;
    boolean getNextPointer = false;

    if (tsColId != -1) {
      long prevBlockId = this.rawDAO.getPreviousBlockId(tableId, pointer.getKey());
      if (prevBlockId != pointer.getKey() & prevBlockId != 0) {
        isStarted = false;

        long[] timestamps = rawDAO.getRawLong(tableId, prevBlockId, tsColId);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] == pointer.getKey()) {
            pointer = Map.entry(prevBlockId, i);
          }
        }
      }
    }

    ColumnKey columnKeyBegin = ColumnKey.builder().tableId(tableId).blockId(pointer.getKey()).colId(0).build();
    ColumnKey columnKeyEnd = ColumnKey.builder().tableId(tableId).blockId(maxBlockId).colId(0).build();
    EntityCursor<CMetadata> cursor = rawDAO.getCMetadataEntityCursor(columnKeyBegin, columnKeyEnd);

    try (cursor) {
      CMetadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getColumnKey().getBlockId();

        if (getNextPointer) {
          return Map.entry(Map.entry(blockId, 0), columnData);
        }

        if (cProfile.getCsType().isTimeStamp()) { // timestamp
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          long[] timestamps = rawDAO.getRawLong(tableId, blockId, cProfile.getColId());

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(timestamps[i]);
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(blockId, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.RAW & !cProfile.getCsType().isTimeStamp()) { // raw data
          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          String[] column = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
              tableId, blockId, cProfile.getColId());

          for (int i = startPoint; i < column.length; i++) {
            columnData.add(column[i]);
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == column.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(blockId, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.HISTOGRAM) { // indexed data
          long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

          int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(blockId, i + 1), columnData);
              }
            }
          }

          if (isPointerFirst) isPointerFirst = false;
        }

        if (cProfile.getCsType().getSType() == SType.ENUM) { // enum data
          long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

          EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

          int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

          for (int i = startPoint; i < timestamps.length; i++) {
            columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), eColumn.getDataByte()[i]), cProfile));
            fetchCounter.decrementAndGet();
            if (fetchCounter.get() == 0) {
              if (i == timestamps.length - 1) {
                getNextPointer = true;
              } else {
                return Map.entry(Map.entry(blockId, i + 1), columnData);
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

    return Map.entry(Map.entry(maxBlockId + 1, 0), columnData);
  }

  @Override
  public long getMaxBlockId(byte tableId) {
    return rawDAO.getLastBlockId(tableId);
  }

  private List<List<Object>> getRawData(String tableName, List<CProfile> cProfiles, long begin, long end) {
    byte tableId = getTableId(tableName, metaModel);
    CProfile tsProfile = getTsProfile(tableName);

    List<List<Object>> columnDataListOut = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, previousBlockId, begin, end, columnDataListOut);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
            this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, blockId, begin, end, columnDataListOut));

    return columnDataListOut;
  }

  private CProfile getTsProfile(String tableName) {
    return getCProfiles(tableName, metaModel).stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findFirst()
        .orElseThrow();
  }

  private void computeRawDataBeginEnd(byte tableId, CProfile tsProfile, List<CProfile> cProfiles,
      long blockId, long begin, long end, List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

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
            tableId, blockId, cProfile.getColId());

        IntStream iRow = IntStream.range(0, timestamps.length);
        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            columnData.add(column[iR]);
          }
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

      if (cProfile.getCsType().getSType() == SType.HISTOGRAM) { // indexed data
        int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

      if (cProfile.getCsType().getSType() == SType.ENUM) { // enum data

        EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

        IntStream iRow = IntStream.range(0, timestamps.length);

        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), eColumn.getDataByte()[iR]), cProfile));
          }
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }

  private void computeNoIndexBeginEnd(byte tableId, CProfile tProfile, CProfile cProfile,
      long blockId, long begin, long end, List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, blockId, tProfile.getColId());

    String[] column = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
        tableId, blockId, cProfile.getColId());

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(column[iR], (k, val) -> val == null ? 1 : val + 1);
      }
    });

    list.add(StackedColumn.builder()
        .key(blockId)
        .tail(tail)
        .keyCount(map).build());
  }

}
