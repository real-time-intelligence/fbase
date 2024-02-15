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
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.RawService;
import org.fbase.sql.BatchResultSet;
import org.fbase.sql.BatchResultSetImpl;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class RawServiceImpl extends CommonServiceApi implements RawService {
  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final HistogramDAO histogramDAO;
  private final EnumDAO enumDAO;

  public RawServiceImpl(MetaModelApi metaModelApi,
                        Converter converter,
                        RawDAO rawDAO,
                        HistogramDAO histogramDAO,
                        EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.histogramDAO = histogramDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName,
                                                  CProfile cProfile,
                                                  long begin,
                                                  long end)
      throws SqlColMetadataException {
    byte tableId = metaModelApi.getTableId(tableName);
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

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
        .forEach(blockId -> this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, blockId, begin, end, list));

    return list;
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          long begin,
                                          long end) {
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName,
                                               CProfile cProfile,
                                               long begin,
                                               long end) {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);
    List<CProfile> cProfiles = List.of(tsProfile, cProfile);
    return getRawData(tableName, cProfiles, begin, end);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize) {
    byte tableId = metaModelApi.getTableId(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    return new BatchResultSetImpl(tableName, tableId, fetchSize, begin, end, cProfiles, this);
  }

  @Override
  public Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId,
                                                                 int colId,
                                                                 int tsColId,
                                                                 CProfile cProfile,
                                                                 int fetchSize,
                                                                 boolean isStarted,
                                                                 long maxBlockId,
                                                                 Entry<Long, Integer> pointer,
                                                                 AtomicInteger fetchCounter) {

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

    MetadataKey beginMK = MetadataKey.builder().tableId(tableId).blockId(pointer.getKey()).build();
    MetadataKey endMK = MetadataKey.builder().tableId(tableId).blockId(maxBlockId).build();
    EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(beginMK, endMK);

    try (cursor) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

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

          if (isPointerFirst) {
            isPointerFirst = false;
          }
        } else {
          SType sType = getSType(colId, columnKey);

          if (SType.RAW.equals(sType) & !cProfile.getCsType().isTimeStamp()) { // raw data
            int startPoint = isStarted ? 0 : isPointerFirst ? pointer.getValue() : 0;

            String[] column = getStringArrayValue(rawDAO, tableId, blockId, cProfile);

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

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }

          if (SType.HISTOGRAM.equals(sType)) { // indexed data
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

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }

          if (SType.ENUM.equals(sType)) { // enum data
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

            if (isPointerFirst) {
              isPointerFirst = false;
            }
          }
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

  @Override
  public long getLastTimestamp(String tableName,
                               long begin,
                               long end) {
    byte tableId = metaModelApi.getTableId(tableName);

    return this.rawDAO.getLastBlockId(tableId, begin, end);
  }

  private List<List<Object>> getRawData(String tableName,
                                        List<CProfile> cProfiles,
                                        long begin,
                                        long end) {
    byte tableId = metaModelApi.getTableId(tableName);
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    List<List<Object>> columnDataList = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, previousBlockId, begin, end, columnDataList);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, blockId, begin, end, columnDataList));

    return columnDataList;
  }

  private void computeRawDataBeginEnd(byte tableId,
                                      CProfile tsProfile,
                                      List<CProfile> cProfiles,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    cProfiles.forEach(cProfile -> {

      List<Object> columnData = new ArrayList<>();
      if (cProfile.getCsType().isTimeStamp()) { // timestamp

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            columnData.add(timestamps[i]);
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 0 : cProfile.getColId(), columnData);
      } else {

        MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();

        SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));

        if (SType.RAW.equals(sType) & !cProfile.getCsType().isTimeStamp()) { // raw data

          String[] column = getStringArrayValue(rawDAO, tableId, blockId, cProfile);

          if (column.length != 0) {
            IntStream iRow = IntStream.range(0, timestamps.length);
            iRow.forEach(iR -> {
              if (timestamps[iR] >= begin & timestamps[iR] <= end) {
                columnData.add(column[iR]);
              }
            });
          }

          columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
        }

        if (SType.HISTOGRAM.equals(sType)) { // indexed data
          int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

          for (int i = 0; i < timestamps.length; i++) {
            if (timestamps[i] >= begin & timestamps[i] <= end) {
              columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
            }
          }

          columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
        }

        if (SType.ENUM.equals(sType)) { // enum data

          EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

          IntStream iRow = IntStream.range(0, timestamps.length);

          iRow.forEach(iR -> {
            if (timestamps[iR] >= begin & timestamps[iR] <= end) {
              columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), eColumn.getDataByte()[iR]), cProfile));
            }
          });

          columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), columnData);
        }
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }

  private void computeNoIndexBeginEnd(byte tableId,
                                      CProfile tProfile,
                                      CProfile cProfile,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, blockId, tProfile.getColId());

    String[] column = getStringArrayValue(rawDAO, tableId, blockId, cProfile);

    long tail = timestamps[timestamps.length - 1];

    if (column.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          map.compute(column[iR], (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(map).build());
    }
  }
}
