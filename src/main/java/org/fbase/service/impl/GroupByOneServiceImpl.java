package org.fbase.service.impl;

import static org.fbase.metadata.DataType.ARRAY;
import static org.fbase.metadata.DataType.MAP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByOneService;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.MetadataKey;

@Log4j2
public class GroupByOneServiceImpl extends CommonServiceApi implements GroupByOneService {

  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByOneServiceImpl(MetaModelApi metaModelApi,
                               Converter converter,
                               HistogramDAO histogramDAO,
                               RawDAO rawDAO,
                               EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName,
                                                  CProfile cProfile,
                                                  GroupFunction groupFunction,
                                                  long begin,
                                                  long end) throws SqlColMetadataException {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    if (GroupFunction.COUNT.equals(groupFunction)) {
      return this.getListStackedColumnCount(tableName, tsProfile, cProfile, null, null, null, begin, end);
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return this.getListStackedColumnSum(tableName, tsProfile, cProfile, groupFunction, null, null, null, begin, end);
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return this.getListStackedColumnAvg(tableName, tsProfile, cProfile, groupFunction, null, null, null, begin, end);
    } else {
      throw new RuntimeException("Group function not supported: " + groupFunction);
    }
  }

  @Override
  public List<StackedColumn> getListStackedColumnFilter(String tableName,
                                                        CProfile cProfile,
                                                        GroupFunction groupFunction,
                                                        CProfile cProfileFilter,
                                                        String[] filterData,
                                                        CompareFunction compareFunction,
                                                        long begin,
                                                        long end) throws SqlColMetadataException {
    CProfile tsProfile = metaModelApi.getTimestampCProfile(tableName);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    if (GroupFunction.COUNT.equals(groupFunction)) {
      return this.getListStackedColumnCount(tableName, tsProfile, cProfile, cProfileFilter, filterData, compareFunction, begin, end);
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return this.getListStackedColumnSum(tableName, tsProfile, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end);
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return this.getListStackedColumnAvg(tableName, tsProfile, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end);
    } else {
      throw new RuntimeException("Group function not supported: " + groupFunction);
    }
  }

  private List<StackedColumn> getListStackedColumnSum(String tableName,
                                                      CProfile tsProfile,
                                                      CProfile cProfile,
                                                      GroupFunction groupFunction,
                                                      CProfile cProfileFilter,
                                                      String[] filterData,
                                                      CompareFunction compareFunction,
                                                      long begin,
                                                      long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeySum(new HashMap<>());

    List<Object> columnData = getColumnData(tableId, tsProfile, cProfile, cProfileFilter, filterData, compareFunction, begin, end);
    if (!columnData.isEmpty()) {
      double sum = columnData.stream()
          .filter(item -> item != null && !((String) item).isEmpty())
          .mapToDouble(item -> Double.parseDouble((String) item))
          .sum();

      stackedColumn.getKeySum().put(cProfile.getColName(), sum);
    }

    return List.of(stackedColumn);
  }

  private List<StackedColumn> getListStackedColumnAvg(String tableName,
                                                      CProfile tsProfile,
                                                      CProfile cProfile,
                                                      GroupFunction groupFunction,
                                                      CProfile cProfileFilter,
                                                      String[] filterData,
                                                      CompareFunction compareFunction,
                                                      long begin,
                                                      long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeyAvg(new HashMap<>());

    List<Object> columnData = getColumnData(tableId, tsProfile, cProfile, cProfileFilter, filterData, compareFunction, begin, end);

    OptionalDouble average = columnData.stream()
        .filter(item -> item != null && !((String) item).isEmpty())
        .mapToDouble(item -> Double.parseDouble((String) item))
        .average();

    if (average.isPresent()) {
      stackedColumn.getKeyAvg().put(cProfile.getColName(), average.getAsDouble());
    }

    return List.of(stackedColumn);
  }

  private List<Object> getColumnData(byte tableId,
                                     CProfile tsProfile,
                                     CProfile cProfile,
                                     CProfile cProfileFilter,
                                     String[] filterData,
                                     CompareFunction compareFunction,
                                     long begin,
                                     long end) {
    List<Object> columnData = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin && previousBlockId != 0) {
      this.computeFilteredRawData(tableId, tsProfile, cProfile, cProfileFilter, filterData, compareFunction, previousBlockId, begin, end, columnData);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeFilteredRawData(tableId, tsProfile, cProfile, cProfileFilter, filterData, compareFunction, blockId, begin, end, columnData));

    return columnData;
  }

  private void computeFilteredRawData(byte tableId,
                                      CProfile tsProfile,
                                      CProfile cProfile,
                                      CProfile cProfileFilter,
                                      String[] filterData,
                                      CompareFunction compareFunction,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<Object> columnData) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

    MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();

    String[] columnValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfile, blockId, timestamps);
    String[] filterValues = null;
    if (cProfileFilter != null) {
      filterValues = getStringArrayValues(rawDAO, enumDAO, histogramDAO, converter, tableId, cProfileFilter, blockId, timestamps);
    }

    for (int i = 0; i < timestamps.length; i++) {
      if (timestamps[i] >= begin && timestamps[i] <= end) {
        boolean shouldInclude = (filterValues == null) ||
            filter(filterValues[i], filterData, compareFunction);
        if (shouldInclude) {
          columnData.add(columnValues[i]);
        }
      }
    }
  }

  private List<StackedColumn> getListStackedColumnCount(String tableName,
                                                        CProfile tsProfile,
                                                        CProfile cProfile,
                                                        CProfile cProfileFilter,
                                                        String[] filterData,
                                                        CompareFunction compareFunction,
                                                        long begin,
                                                        long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getStacked(tableName, tsProfile, cProfile, GroupFunction.COUNT,
                               cProfileFilter, filterData, compareFunction, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin && previousBlockId != 0) {
      processBlock(tableId, tsProfile, cProfile, cProfileFilter, filterData,
                   compareFunction, previousBlockId, begin, end, list);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end).forEach(blockId ->
                                                                 processBlock(tableId, tsProfile, cProfile, cProfileFilter, filterData,
                                                                              compareFunction, blockId, begin, end, list)
    );

    if (MAP.equals(cProfile.getCsType().getDType())) {
      return handleMap(list);
    } else if (ARRAY.equals(cProfile.getCsType().getDType())) {
      return handleArray(list);
    }

    return list;
  }

  private void processBlock(byte tableId,
                            CProfile tsProfile,
                            CProfile cProfile,
                            CProfile cProfileFilter,
                            String[] filterData,
                            CompareFunction compareFunction,
                            long blockId,
                            long begin,
                            long end,
                            List<StackedColumn> list) {
    try {
      long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

      String[] columnValues = getStringArrayValues(
          rawDAO, enumDAO, histogramDAO, converter,
          tableId, cProfile, blockId, timestamps
      );

      String[] filterValues = null;
      if (cProfileFilter != null) {
        filterValues = getStringArrayValues(
            rawDAO, enumDAO, histogramDAO, converter,
            tableId, cProfileFilter, blockId, timestamps
        );
      }

      Map<String, Integer> valueCounts = new LinkedHashMap<>();
      long lastTimestamp = timestamps.length > 0 ? timestamps[timestamps.length - 1] : 0;

      for (int i = 0; i < timestamps.length; i++) {
        long timestamp = timestamps[i];

        if (timestamp >= begin && timestamp <= end) {
          boolean includeRow = true;

          if (filterValues != null) {
            includeRow = filter(filterValues[i], filterData, compareFunction);
          }

          if (includeRow) {
            String value = columnValues[i] != null ? columnValues[i] : "";
            valueCounts.compute(value, (k, count) -> count == null ? 1 : count + 1);
          }
        }
      }

      if (!valueCounts.isEmpty()) {
        StackedColumn column = StackedColumn.builder()
            .key(blockId)
            .tail(lastTimestamp)
            .keyCount(valueCounts)
            .build();
        list.add(column);
      }
    } catch (Exception e) {
      log.error("Error processing block {}: {}", blockId, e.getMessage(), e);
    }
  }
}