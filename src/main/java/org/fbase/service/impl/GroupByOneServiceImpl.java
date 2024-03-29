package org.fbase.service.impl;

import static org.fbase.metadata.DataType.ARRAY;
import static org.fbase.metadata.DataType.MAP;

import com.sleepycat.persist.EntityCursor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByOneService;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.fbase.storage.helper.EnumHelper;

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
      return this.getListStackedColumnCount(tableName, tsProfile, cProfile, null, null, begin, end);
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return this.getListStackedColumnSum(tableName, tsProfile, cProfile, groupFunction, begin, end);
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return this.getListStackedColumnAvg(tableName, tsProfile, cProfile, groupFunction, begin, end);
    } else {
      throw new RuntimeException("Group function not supported: " + groupFunction);
    }
  }

  @Override
  public List<StackedColumn> getListStackedColumnFilter(String tableName,
                                                        CProfile cProfile,
                                                        GroupFunction groupFunction,
                                                        CProfile cProfileFilter,
                                                        String filter,
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
      return this.getListStackedColumnCount(tableName, tsProfile, cProfile, cProfileFilter, filter, begin, end);
    } else if (GroupFunction.SUM.equals(groupFunction) | GroupFunction.AVG.equals(groupFunction)) {
      throw new RuntimeException("Not supported for: " + groupFunction);
    } else {
      throw new RuntimeException("Group function not supported: " + groupFunction);
    }
  }

  private List<StackedColumn> getListStackedColumnSum(String tableName,
                                                      CProfile tsProfile,
                                                      CProfile cProfile,
                                                      GroupFunction groupFunction,
                                                      long begin,
                                                      long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getListStackedColumn(tableName, tsProfile, cProfile, groupFunction, null, null, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeySum(new HashMap<>());

    List<Object> columnData = getColumnData(tableId, tsProfile, cProfile, begin, end);
    if (!columnData.isEmpty()) {
      double sum = columnData.stream()
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
                                                      long begin,
                                                      long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getListStackedColumn(tableName, tsProfile, cProfile, groupFunction, null, null, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    StackedColumn stackedColumn = StackedColumn.builder().key(begin).tail(end).build();
    stackedColumn.setKeyAvg(new HashMap<>());

    List<Object> columnData = getColumnData(tableId, tsProfile, cProfile, begin, end);

    OptionalDouble average = columnData.stream()
        .mapToDouble(item -> Double.parseDouble((String) item))
        .average();

    if (average.isPresent()) {
      stackedColumn.getKeyAvg().put(cProfile.getColName(), average.getAsDouble());
    }

    return List.of(stackedColumn);
  }

  private List<Object> getColumnData(byte tableId, CProfile tsProfile, CProfile cProfile, long begin, long end) {
    List<Object> columnData = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfile, previousBlockId, begin, end, columnData);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeRawDataBeginEnd(tableId, tsProfile, cProfile, blockId, begin, end, columnData));

    return columnData;
  }


  private void computeRawDataBeginEnd(byte tableId,
                                      CProfile tsProfile,
                                      CProfile cProfile,
                                      long blockId,
                                      long begin,
                                      long end,
                                      List<Object> columnData) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

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
    }

    if (SType.HISTOGRAM.equals(sType)) { // indexed data
      int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

      for (int i = 0; i < timestamps.length; i++) {
        if (timestamps[i] >= begin & timestamps[i] <= end) {
          columnData.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
        }
      }
    }

    if (SType.ENUM.equals(sType)) { // enum data
      EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

      IntStream iRow = IntStream.range(0, timestamps.length);

      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          columnData.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), eColumn.getDataByte()[iR]), cProfile));
        }
      });
    }
  }

  private List<StackedColumn> getListStackedColumnCount(String tableName,
                                                        CProfile tsProfile,
                                                        CProfile cProfile,
                                                        CProfile cProfileFilter,
                                                        String filter,
                                                        long begin,
                                                        long end) {
    BType bType = metaModelApi.getBackendType(tableName);

    if (!BType.BERKLEYDB.equals(bType)) {
      return rawDAO.getListStackedColumn(tableName, tsProfile, cProfile, GroupFunction.COUNT, cProfileFilter, filter, begin, end);
    }

    byte tableId = metaModelApi.getTableId(tableName);

    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

        if (tsProfile.getColId() == cProfile.getColId()) {
          this.computeRaw(tableId, cProfile, blockId, timestamps, begin, end, list);
        } else {
          SType sType = getSType(cProfile.getColId(), columnKey);

          if (SType.RAW.equals(sType)) {
            this.computeRaw(tableId, cProfile, cProfileFilter, filter, blockId, timestamps, begin, end, list);
          }

          if (SType.HISTOGRAM.equals(sType)) {
            if (cProfileFilter == null) {
              computeHist(tableId, blockId, cProfile, timestamps, begin, end, list);
            } else {
              computeHist(tableId, cProfile, cProfileFilter, filter, blockId, timestamps, begin, end, list);
            }
          }

          if (SType.ENUM.equals(sType)) {
            if (cProfileFilter == null) {
              this.computeEnum(tableId, cProfile, blockId, timestamps, begin, end, list);
            } else {
              this.computeEnum(tableId, cProfile, cProfileFilter, filter, blockId, timestamps, begin, end, list);
            }
          }
        }
      }
    } catch (Exception e) {
      log.catching(e);
      log.error(e.getMessage());
    }

    if (MAP.equals(cProfile.getCsType().getDType())) {
      return handleMap(list);
    } else if (ARRAY.equals(cProfile.getCsType().getDType())) {
      return handleArray(list);
    }

    return list;
  }

  private void computeRaw(byte tableId,
                          CProfile cProfile,
                          long blockId,
                          long[] timestamps,
                          long begin,
                          long end,
                          List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

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

  private void computeRaw(byte tableId,
                          CProfile cProfile,
                          CProfile cProfileFilter,
                          String filter,
                          long blockId,
                          long[] timestamps,
                          long begin,
                          long end,
                          List<StackedColumn> list) {
    String[] columnValues = getStringArrayValue(rawDAO, tableId, blockId, cProfile);
    String[] filterValues = null;
    if (cProfileFilter != null) {
      filterValues = getArray(tableId, cProfileFilter, blockId, timestamps);
    }

    long tail = timestamps[timestamps.length - 1];
    Map<String, Integer> map = new LinkedHashMap<>();

    for (int i = 0; i < timestamps.length; i++) {
      if (timestamps[i] >= begin && timestamps[i] <= end) {
        boolean shouldFilter = filterValues == null || filterValues[i].equals(filter);

        if (shouldFilter) {
          map.compute(columnValues[i], (k, val) -> val == null ? 1 : val + 1);
        }
      }
    }

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(map).build());
    }
  }

  private void computeEnum(byte tableId,
                           CProfile cProfile,
                           long blockId,
                           long[] timestamps,
                           long begin,
                           long end,
                           List<StackedColumn> list) {
    Map<Byte, Integer> map = new LinkedHashMap<>();

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(eColumn.getDataByte()[iR], (k, val) -> val == null ? 1 : val + 1);
      }
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();

    map.forEach((keyByte, value) -> mapKeyCount.put(converter.convertIntToRaw(
        EnumHelper.getIndexValue(eColumn.getValues(), keyByte), cProfile), value));

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(mapKeyCount).build());
    }
  }

  private void computeEnum(byte tableId,
                           CProfile cProfile,
                           CProfile cProfileFilter,
                           String filter,
                           long blockId,
                           long[] timestamps,
                           long begin,
                           long end,
                           List<StackedColumn> list) {
    Map<String, Integer> map = new LinkedHashMap<>();

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    String[] array = getArray(tableId, cProfileFilter, blockId, timestamps);

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {

        byte keyByte = eColumn.getDataByte()[iR];
        String valueStr = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), keyByte), cProfile);

        if (filter.equals(array[iR])) {
          map.compute(valueStr, (k, val) -> val == null ? 1 : val + 1);
        }
      }
    });

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(map).build());
    }
  }

  private String[] getArray(byte tableId,
                            CProfile cProfileFilter,
                            long blockId,
                            long[] timestamps) {
    MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();
    SType sType = getSType(cProfileFilter.getColId(), rawDAO.getMetadata(metadataKey));

    if (SType.RAW.equals(sType)) {
      return getStringArrayValue(rawDAO, tableId, blockId, cProfileFilter);
    } else if (SType.ENUM.equals(sType)) {
      return getArrayForEnum(tableId, cProfileFilter, blockId, timestamps);
    } else if (SType.HISTOGRAM.equals(sType)) {
      return getArrayForHist(tableId, cProfileFilter, blockId, timestamps);
    }

    return new String[0];
  }

  private String[] getArrayForEnum(byte tableId,
                                   CProfile cProfileFilter,
                                   long blockId,
                                   long[] timestamps) {
    String[] array = new String[timestamps.length];

    EColumn eColumnFilter = enumDAO.getEColumnValues(tableId, blockId, cProfileFilter.getColId());

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      byte filterByte = eColumnFilter.getDataByte()[iR];
      array[iR] = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumnFilter.getValues(), filterByte), cProfileFilter);
    });

    return array;
  }

  private String[] getArrayForHist(byte tableId,
                                   CProfile cProfileFilter,
                                   long blockId,
                                   long[] timestamps) {
    String[] array = new String[timestamps.length];

    int[][] histograms = histogramDAO.get(tableId, blockId, cProfileFilter.getColId());
    int[] histogramsUnPack = getHistogramUnPack(timestamps, histograms);

    IntStream iRow = IntStream.range(0, timestamps.length);

    iRow.forEach(iR -> array[iR] = this.converter.convertIntToRaw(histogramsUnPack[iR], cProfileFilter));

    return array;
  }

  private void computeHist(byte tableId,
                           long blockId,
                           CProfile firstGrpBy,
                           long[] timestamps,
                           long begin,
                           long end,
                           List<StackedColumn> list) {
    Map<Integer, Integer> map = new LinkedHashMap<>();

    int[][] f = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());

    boolean checkRange = timestamps[f[0][0]] >= begin & timestamps[f[0][f[0].length - 1]] <= end;

    for (int i = 0; i < f[0].length; i++) {
      int deltaCountValue;

      if (i == f[0].length - 1) { //todo last row
        if (f[0][i] == 0) {
          deltaCountValue = 1;
        } else {
          deltaCountValue = timestamps.length - f[0][i];
        }
      } else {
        deltaCountValue = f[0][i + 1] - f[0][i];
      }

      int fNextIndex = getNextIndex(i, f, timestamps);

      if (checkRange) {
        map.compute(f[1][i], (k, val) -> val == null ? deltaCountValue : val + deltaCountValue);
      } else {
        for (int iR = f[0][i]; (f[0][i] == fNextIndex) ? iR < fNextIndex + 1 : iR <= fNextIndex; iR++) {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            map.compute(f[1][i], (k, val) -> val == null ? 1 : val + 1);
          }
        }
      }
    }

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();
    map.forEach((keyInt, value) -> mapKeyCount.put(this.converter.convertIntToRaw(keyInt, firstGrpBy), value));

    long tail = timestamps[timestamps.length - 1];
    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(mapKeyCount).build());
    }
  }

  private void computeHist(byte tableId,
                           CProfile cProfile,
                           CProfile cProfileFilter,
                           String filter,
                           long blockId,
                           long[] timestamps,
                           long begin,
                           long end,
                           List<StackedColumn> list) {
    Map<String, Integer> map = new LinkedHashMap<>();

    String[] array = getArray(tableId, cProfile, blockId, timestamps);
    String[] arrayFilter = getArray(tableId, cProfileFilter, blockId, timestamps);

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        if (filter.equals(arrayFilter[iR])) {
          map.compute(array[iR], (k, val) -> val == null ? 1 : val + 1);
        }
      }
    });

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
                   .key(blockId)
                   .tail(tail)
                   .keyCount(map).build());
    }
  }
}