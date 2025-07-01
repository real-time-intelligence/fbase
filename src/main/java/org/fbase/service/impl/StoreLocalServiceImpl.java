package org.fbase.service.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.StatisticsService;
import org.fbase.service.StoreLocalService;
import org.fbase.service.mapping.Mapper;
import org.fbase.service.store.TStore;
import org.fbase.service.store.UStore;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.util.CachedLastLinkedHashMap;

@Log4j2
public class StoreLocalServiceImpl extends CommonServiceApi implements StoreLocalService {
  private final MetaModelApi metaModelApi;

  private final Converter converter;

  private final EnumDAO enumDAO;

  private final RawDAO rawDAO;

  private final HistogramDAO histogramDAO;
  private final StatisticsService statisticsService;

  public StoreLocalServiceImpl(MetaModelApi metaModelApi,
                          StatisticsService statisticsService,
                          Converter converter,
                          RawDAO rawDAO,
                          EnumDAO enumDAO,
                          HistogramDAO histogramDAO) {
    this.metaModelApi = metaModelApi;
    this.statisticsService = statisticsService;
    this.converter = converter;

    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
    this.histogramDAO = histogramDAO;
  }

  @Override
  public void putDataDirect(String tableName,
                            List<List<Object>> data) {

  }

  private long putDataJdbcLocal(String tableName,
                                ResultSet resultSet) {
    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(),
                                               statisticsService.getLastSType(tableId,
                                                                              cProfile.getColId(),
                                                                              cProfile.getCsType().isTimeStamp())));

    // Initialize stores
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    Map<Integer, CProfile> colIdToProfile = new HashMap<>();
    cProfiles.forEach(p -> colIdToProfile.put(p.getColId(), p));

    try {
      boolean isStatByTableExist = statisticsService.isStatByTableExist(tableId);

      AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndIncrement();

        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }

        /** Update SType statistic **/
        if (!isStatByTableExist && iR == 100) {
          uStore.analyzeAndConvertColumns(iR, colIdToProfile);
          isStatByTableExist = true;
        }
      }

      /** Update SType statistic **/
      if (!isStatByTableExist) {
        uStore.analyzeAndConvertColumns(iRow.get(), colIdToProfile);
      }

      if (tStore.size() == 0) return -1;
      long blockId = tStore.getBlockId();

      // Store data using UStore
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);

      // Full mode analysis after storing data
      // TODO: Make this configurable in settings (disabled by default for better performance)
      // fullModeColumnAnalyze(tableId, cProfiles, uStore, iRow);

      return tStore.getTail();
    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void fullModeColumnAnalyze(byte tableId,
                         List<CProfile> cProfiles,
                         UStore uStore,
                         AtomicInteger iRow) {
    List<CProfile> nonTimestampColumns = cProfiles.stream()
        .filter(isNotTimestamp)
        .collect(Collectors.toList());

    if (!nonTimestampColumns.isEmpty() && statisticsService instanceof StatisticsServiceImpl) {
      StatisticsServiceImpl statisticsServiceImpl = (StatisticsServiceImpl) statisticsService;
      int colIdToAnalyze = getNextColumnToAnalyze(tableId, nonTimestampColumns, statisticsServiceImpl);

      CProfile cProfile = nonTimestampColumns.stream()
          .filter(c -> c.getColId() == colIdToAnalyze)
          .findFirst()
          .orElse(null);

      if (cProfile != null) {
        SType newSType = uStore.analyzeColumn(colIdToAnalyze, iRow.get(), cProfile);
        statisticsServiceImpl.updateSType(tableId, colIdToAnalyze, newSType);
      }
    }
  }

  private int getNextColumnToAnalyze(byte tableId,
                                     List<CProfile> nonTimestampColumns,
                                     StatisticsServiceImpl statisticsService) {
    Integer lastColId = statisticsService.getLastAnalyzedColId(tableId);
    int index = 0;

    if (lastColId != null) {
      for (int i = 0; i < nonTimestampColumns.size(); i++) {
        if (nonTimestampColumns.get(i).getColId() == lastColId) {
          index = (i + 1) % nonTimestampColumns.size();
          break;
        }
      }
    }

    int nextColId = nonTimestampColumns.get(index).getColId();
    statisticsService.setLastAnalyzedColId(tableId, nextColId);
    return nextColId;
  }

  @Override
  public long putDataJdbc(String tableName, ResultSet resultSet) {
    IType indexType = metaModelApi.getIndexType(tableName);
    if (IType.LOCAL.equals(indexType)) {
      log.info(IType.LOCAL);
      return putDataJdbcLocal(tableName, resultSet);
    }

    byte tableId = metaModelApi.getTableId(tableName);
    boolean compression = metaModelApi.getTableCompression(tableName);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableName);
    int colCount = cProfiles.size();

    // Initialize storage type map
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> colIdSTypeMap.put(cProfile.getColId(), cProfile.getCsType().getSType()));

    // Initialize stores
    TStore tStore = new TStore(1, cProfiles);
    UStore uStore = new UStore(converter);

    // Initialize columns in UStore
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(cProfile -> uStore.addColumn(cProfile, colIdSTypeMap.get(cProfile.getColId())));

    try {
      AtomicInteger iRow = new AtomicInteger(0);
      while (resultSet.next()) {
        int iR = iRow.getAndIncrement();

        for (int iC = 0; iC < colCount; iC++) {
          CProfile cProfile = cProfiles.get(iC);
          CSType csType = cProfile.getCsType();
          Object currObject = resultSet.getObject(cProfile.getColIdSql());

          if (csType.isTimeStamp()) {
            tStore.add(iC, iR, converter.getKeyValue(currObject, cProfile));
          } else {
            uStore.add(cProfile, iR, currObject);
          }
        }
      }

      if (tStore.size() == 0) return -1;
      long blockId = tStore.getBlockId();

      // Store data using UStore
      storeDataLocal(tableId, blockId, compression, cProfiles, tStore, uStore, colIdSTypeMap);
      return tStore.getTail();
    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void storeDataLocal(byte tableId,
                              long blockId,
                              boolean compression,
                              List<CProfile> cProfiles,
                              TStore tStore,
                              UStore uStore,
                              Map<Integer, SType> colIdSTypeMap) {
    // Store metadata
    storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    // Store timestamp data
    storeTStore(tableId, blockId, compression, tStore);

    // Store RAW data
    storeRawFromUStore(tableId, blockId, compression, cProfiles, uStore);

    // Store ENUM data
    storeEnumFromUStore(tableId, blockId, compression, uStore);

    // Store HISTOGRAM data
    storeHistogramsEntry(tableId, blockId, compression, uStore);
  }

  private void storeMetadataLocal(byte tableId,
                                  long blockId,
                                  List<CProfile> cProfiles,
                                  Map<Integer, SType> colIdSTypeMap) {
    List<Byte> rawCTypeKeys = new ArrayList<>();
    List<Integer> rawColIds = new ArrayList<>();
    List<Integer> enumColIds = new ArrayList<>();
    List<Integer> histogramColIds = new ArrayList<>();

    cProfiles.forEach(cProfile -> {
      if (SType.RAW.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        rawCTypeKeys.add(cProfile.getCsType().getCType().getKey());
        rawColIds.add(cProfile.getColId());
      } else if (SType.ENUM.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        enumColIds.add(cProfile.getColId());
      } else if (SType.HISTOGRAM.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        histogramColIds.add(cProfile.getColId());
      }
    });

    this.rawDAO.putMetadata(tableId, blockId, getByteFromList(rawCTypeKeys),
                            rawColIds.stream().mapToInt(j -> j).toArray(),
                            enumColIds.stream().mapToInt(j -> j).toArray(),
                            histogramColIds.stream().mapToInt(j -> j).toArray());
  }

  private void storeTStore(byte tableId,
                           long blockId,
                           boolean compression,
                           TStore tStore) {
    if (compression) {
      rawDAO.putCompressed(tableId, blockId, tStore.getMapping(), tStore.getRawData());
    } else {
      rawDAO.putLong(tableId, blockId, tStore.mappingToArray(), tStore.dataToArray());
    }
  }

  private void storeRawFromUStore(byte tableId,
                                  long blockId,
                                  boolean compression,
                                  List<CProfile> cProfiles,
                                  UStore uStore) {

    Map<Integer, CProfile> colIdToProfile = new HashMap<>();
    cProfiles.forEach(p -> colIdToProfile.put(p.getColId(), p));

    Map<CType, Map<Integer, List<Object>>> rawDataByType = new EnumMap<>(CType.class);

    uStore.getRawDataMap().forEach((colId, data) -> {
      if (uStore.getStorageTypeMap().get(colId) == SType.RAW) {
        CType cType = Mapper.isCType(colIdToProfile.get(colId));
        rawDataByType
            .computeIfAbsent(cType, k -> new HashMap<>())
            .put(colId, data);
      }
    });

    if (compression) {
      rawDAO.putCompressed(tableId, blockId, rawDataByType);
    } else {
      rawDataByType.forEach((cType, colDataMap) -> {
        int[] colIds = colDataMap.keySet().stream().mapToInt(i -> i).toArray();

        switch (cType) {
          case INT:
            int[][] intData = colDataMap.values().stream()
                .map(list -> list.stream().mapToInt(i -> (Integer) i).toArray())
                .toArray(int[][]::new);
            rawDAO.putInt(tableId, blockId, colIds, intData);
            break;

          case LONG:
            long[][] longData = colDataMap.values().stream()
                .map(list -> list.stream().mapToLong(l -> (Long) l).toArray())
                .toArray(long[][]::new);
            rawDAO.putLong(tableId, blockId, colIds, longData);
            break;

          case FLOAT:
            float[][] floatData = colDataMap.values().stream()
                .map(list -> {
                  float[] arr = new float[list.size()];
                  for (int i = 0; i < list.size(); i++) {
                    arr[i] = (Float) list.get(i);
                  }
                  return arr;
                })
                .toArray(float[][]::new);
            rawDAO.putFloat(tableId, blockId, colIds, floatData);
            break;

          case DOUBLE:
            double[][] doubleData = colDataMap.values().stream()
                .map(list -> list.stream().mapToDouble(d -> (Double) d).toArray())
                .toArray(double[][]::new);
            rawDAO.putDouble(tableId, blockId, colIds, doubleData);
            break;

          case STRING:
            String[][] stringData = colDataMap.values().stream()
                .map(list -> list.toArray(String[]::new))
                .toArray(String[][]::new);
            rawDAO.putString(tableId, blockId, colIds, stringData);
            break;
        }
      });
    }
  }

  private void storeEnumFromUStore(byte tableId,
                                   long blockId,
                                   boolean compression,
                                   UStore uStore) {
    uStore.getEnumDataMap().forEach((colId, byteList) -> {
      CachedLastLinkedHashMap<Integer, Byte> dictionary = uStore.getEnumDictionaries().get(colId);

      int[] values = getIntegerFromSet(dictionary.keySet());

      byte[] data = getByteFromList(byteList);

      try {
        enumDAO.putEColumn(tableId, blockId, colId, values, data, compression);
      } catch (IOException e) {
        log.error("Error storing enum data", e);
        throw new RuntimeException(e);
      }
    });
  }

  private void storeHistogramsEntry(byte tableID,
                                    long blockId,
                                    boolean compression,
                                    UStore uStore) {

    uStore.getHistogramDataMap().forEach((colId, hEntry) -> {
      if (!hEntry.getIndex().isEmpty()) {
        if (compression) {
          this.histogramDAO.putCompressedKeysValues(tableID, blockId, colId,
                                                    hEntry.getIndex().stream().mapToInt(Integer::intValue).toArray(),
                                                    hEntry.getValue().stream().mapToInt(Integer::intValue).toArray());
        } else {
          this.histogramDAO.put(tableID, blockId, colId, getArrayFromMapHEntry(hEntry));
        }
      }
    });

  }

  @Override
  public void putDataJdbcBatch(String tableName,
                               ResultSet resultSet,
                               Integer fBaseBatchSize) {

  }

  @Override
  public void putDataCsvBatch(String tableName,
                              String fileName,
                              String csvSplitBy,
                              Integer fBaseBatchSize) {

  }
}
