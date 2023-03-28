package org.fbase.service.impl;

import static org.fbase.service.mapping.Mapper.INT_NULL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.StoreService;
import org.fbase.service.mapping.Mapper;
import org.fbase.service.store.EStore;
import org.fbase.service.store.HEntry;
import org.fbase.service.store.RStore;
import org.fbase.service.store.TStore;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.helper.EnumHelper;
import org.fbase.util.CachedLastLinkedHashMap;

@Log4j2
public class StoreServiceImpl extends CommonServiceApi implements StoreService {

  private final MetaModel metaModel;

  private final Converter converter;

  private final EnumDAO enumDAO;

  private final RawDAO rawDAO;

  private final HistogramDAO histogramDAO;

  public StoreServiceImpl(MetaModel metaModel, Converter converter, RawDAO rawDAO, EnumDAO enumDAO, HistogramDAO histogramDAO) {
    this.metaModel = metaModel;

    this.converter = converter;

    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
    this.histogramDAO = histogramDAO;
  }

  @Override
  public void putDataDirect(String tableName, List<List<Object>> data) {
    byte tableId = getTableId(tableName, metaModel);
    int rowCount = data.get(0).size();

    boolean compression = getTableCompression(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    int colCount = cProfiles.size();

    /* Timestamp */
    int colRawDataTimeStampCount = 1;
    long[][] rawDataTimestamp = new long[colRawDataTimeStampCount][rowCount];
    List<Integer> rawDataTimeStampMapping = new ArrayList<>(colRawDataTimeStampCount);
    fillTimestampMapping(cProfiles, rawDataTimeStampMapping);

    /* Int */
    int colRawDataIntCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isInt);
    int[][] rawDataInt = new int[colRawDataIntCount][rowCount];
    List<Integer> rawDataIntMapping = new ArrayList<>(colRawDataIntCount);
    fillMappingRaw(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isLong);
    long[][] rawDataLong = new long[colRawDataLongCount][rowCount];
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMappingRaw(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isFloat);
    float[][] rawDataFloat = new float[colRawDataFloatCount][rowCount];
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillMappingRaw(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isDouble);
    double[][] rawDataDouble = new double[colRawDataDoubleCount][rowCount];
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMappingRaw(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isString);
    String[][] rawDataString = new String[colRawDataStringCount][rowCount];
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMappingRaw(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

    /* Enums */
    int colRawDataEnumCount = (int) cProfiles.stream().filter(isEnum).count();
    byte[][] rawDataEnum = new byte[colRawDataEnumCount][rowCount];
    List<Integer> rawDataEnumMapping = new ArrayList<>(colRawDataEnumCount);
    List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

    IntStream iRow = IntStream.range(0, rowCount);
    iRow.forEach(iR -> {
      IntStream iColumn = IntStream.range(0, colCount);
      iColumn.forEach(iC -> {

        CSType csType = cProfiles.get(iC).getCsType();

        // Fill timestamps
        if (csType.isTimeStamp()) {
          if (csType.getSType() == SType.RAW){
            rawDataTimestamp[rawDataTimeStampMapping.indexOf(iC)][iR] =
                this.converter.getKeyValue(data.get(iC).get(iR), cProfiles.get(iC));
          }
        }

        // Fill raw data
        if (csType.getSType() == SType.RAW) {
          if (!csType.isTimeStamp()) {
            if (CType.INT == Mapper.isCType(cProfiles.get(iC))) {
              rawDataInt[rawDataIntMapping.indexOf(iC)][iR] =
                  Mapper.convertRawToInt(data.get(iC).get(iR), cProfiles.get(iC));
            } else if (CType.LONG == Mapper.isCType(cProfiles.get(iC))) {
              rawDataLong[rawDataLongMapping.indexOf(iC)][iR] =
                  Mapper.convertRawToLong(data.get(iC).get(iR), cProfiles.get(iC));
            } else if (CType.FLOAT == Mapper.isCType(cProfiles.get(iC))) {
              rawDataFloat[rawDataFloatMapping.indexOf(iC)][iR] =
                  Mapper.convertRawToFloat(data.get(iC).get(iR), cProfiles.get(iC));
            } else if (CType.DOUBLE == Mapper.isCType(cProfiles.get(iC))) {
              rawDataDouble[rawDataDoubleMapping.indexOf(iC)][iR] =
                  Mapper.convertRawToDouble(data.get(iC).get(iR), cProfiles.get(iC));
            } else if (CType.STRING == Mapper.isCType(cProfiles.get(iC))) {
              rawDataString[rawDataStringMapping.indexOf(iC)][iR] =
                  Mapper.convertRawToString(data.get(iC).get(iR), cProfiles.get(iC));
            }
          }
        }

        // Fill enum data
        if (csType.getSType() == SType.ENUM) {
          int valueInt = this.converter.convertRawToInt(data.get(iC).get(iR), cProfiles.get(iC));

          try {
            rawDataEnum[rawDataEnumMapping.indexOf(iC)][iR] =
                EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt);
          } catch (EnumByteExceedException e) {
            log.error(e.getMessage());
          }

        }

      });
    });

    // Fill histogram data
    Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms = new HashMap<>(colCount);
    cProfiles.stream().filter(isHistogram)
        .forEach(cProfile -> mapOfHistograms.put(cProfile.getColId(), new CachedLastLinkedHashMap<>()));

    cProfiles.stream()
        .filter(f -> f.getCsType().getSType() == SType.HISTOGRAM)
        .forEach(e -> {
          IntStream iRowH = IntStream.range(0, rowCount);

          iRowH.forEach(iR -> {
            int currVar = this.converter.convertRawToInt(data.get(e.getColId()).get(iR), e);

            if (iR != 0) {
              int prevVar = this.converter.convertRawToInt(data.get(e.getColId()).get(iR - 1), e);
              if (prevVar != currVar) {
                mapOfHistograms.get(e.getColId()).put(iR, currVar);
              }
            } else {
              mapOfHistograms.get(e.getColId()).put(iR, currVar);
            }
          });
        });

    long blockId = rawDataTimestamp[0][0];

    /* Store metadata */
    this.storeMetadata(tableId, blockId, cProfiles);

    this.storeHistograms(tableId, compression, blockId, mapOfHistograms);

    this.storeData(tableId, compression, blockId,
        rawDataTimeStampMapping, rawDataTimestamp,
        colRawDataIntCount, rawDataIntMapping, rawDataInt,
        colRawDataLongCount, rawDataLongMapping, rawDataLong,
        colRawDataFloatCount, rawDataFloatMapping, rawDataFloat,
        colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
        colRawDataStringCount, rawDataStringMapping, rawDataString,
        colRawDataEnumCount, rawDataEnumMapping, rawDataEnum, rawDataEnumEColumn);
  }

  @Override
  public long putDataJdbc(String tableName, ResultSet resultSet) {
    IType indexType = getIndexType(tableName, metaModel);
    if (IType.LOCAL.equals(indexType)) {
      log.info(IType.LOCAL);
      return putDataJdbcLocal(tableName, resultSet);
    }

    byte tableId = getTableId(tableName, metaModel);
    boolean compression = getTableCompression(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    int colCount = cProfiles.size();

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    cProfiles.stream().filter(isNotTimestamp)
        .forEach(e -> colIdSTypeMap.put(e.getColId(), e.getCsType().getSType()));

    /* Timestamp */
    TStore tStore = new TStore(1, cProfiles);

    /* Raw */
    RStore rStore = new RStore(converter, cProfiles, colIdSTypeMap);

    /* Enum */
    EStore eStore = new EStore(cProfiles, colIdSTypeMap);

    /* Histogram */
    Map<Integer, HEntry> histograms = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));

    List<Integer> iColumn = IntStream.range(0, colCount).boxed().toList();

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        iColumn.forEach(iC -> {
          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();
            int colId = cProfile.getColId();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW){
                tStore.add(iC, iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            // Fill raw data
            if (csType.getSType() == SType.RAW) {
              if (!csType.isTimeStamp()) {
                rStore.add(cProfile, iR, currObject);
              }
            }

            // Fill enum data
            if (csType.getSType() == SType.ENUM) {
              int curValue = this.converter.convertRawToInt(currObject, cProfile);
              int iMapping = eStore.getMapping().get(colId);
              try {
                 eStore.add(iMapping, iR, curValue);
              } catch (EnumByteExceedException e) {
                 throw new RuntimeException(e);
              }
            }

            // Fill histogram data
            if (csType.getSType() == SType.HISTOGRAM) {
              int prevInt = getPrevValue(histograms, colId);
              int currInt = this.converter.convertRawToInt(currObject, cProfile);

              if (prevInt != currInt) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currInt);
              } else if (iR == 0) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currInt);
              }
            }

          } catch (SQLException e) {
            log.catching(e);
            throw new RuntimeException(e);
          }
        });
      }

      if (tStore.size() == 0) {
        return -1;
      }

      long blockId = tStore.getBlockId();

      this.storeDataGlobal(tableId, compression, blockId, cProfiles, colIdSTypeMap, tStore, rStore, eStore, histograms);

      return tStore.getTail();

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  private long putDataJdbcLocal(String tableName, ResultSet resultSet) {
    byte tableId = getTableId(tableName, metaModel);
    boolean compression = getTableCompression(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    int colCount = cProfiles.size();

    /* Timestamp */
    TStore tStore = new TStore(1, cProfiles);

    // Fill histogram data
    Map<Integer, HEntry> histograms = new HashMap<>();
    cProfiles.stream()
        .filter(isNotTimestamp)
        .forEach(e -> histograms.put(e.getColId(), new HEntry(new ArrayList<>(), new ArrayList<>())));

    List<Integer> iColumn = IntStream.range(0, colCount).boxed().toList();

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        iColumn.forEach(iC -> {

          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW){
                tStore.add(iC, iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            if (!csType.isTimeStamp()) {
              // Fill histogram data
              int colId = cProfile.getColId();
              int prevInt = getPrevValue(histograms, colId);
              int currInt = this.converter.convertRawToInt(currObject, cProfile);

              if (prevInt != currInt) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currInt);
              } else if (iR == 0) {
                histograms.get(colId).getIndex().add(iR);
                histograms.get(colId).getValue().add(currInt);
              }
            }

          } catch (SQLException e) {
            log.catching(e);
            throw new RuntimeException(e);
          }
        });
      }

      log.info("Rows: " + iRow.get());

      if (tStore.size() == 0) {
        return -1;
      }

      long blockId = tStore.getBlockId();

      /* Store data and metadata */
      this.storeDataLocal(tableId, compression, blockId, cProfiles, tStore, histograms);

      return tStore.getTail();

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private int getPrevValue(Map<Integer, HEntry> histograms, int colId) {
    int size = histograms.get(colId).getValue().size();

    if (size == 0) return INT_NULL;

    return histograms.get(colId).getValue().get(size - 1);
  }

  private void storeDataGlobal(byte tableId, boolean compression, long blockId,
      List<CProfile> cProfiles, Map<Integer, SType> colIdSTypeMap,
      TStore tStore, RStore rStore, EStore eStore, Map<Integer, HEntry> histograms) {

    /* Store metadata */
    this.storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    /* Store raw data */
    this.storeRaw(tableId, compression, blockId, tStore, rStore);

    /* Store enum data */
    this.storeEnum(tableId, compression, blockId, eStore);

    /* Store histogram data */
    this.storeHistogramsEntry(tableId, compression, blockId, histograms);
  }

  private void storeRaw(byte tableId, boolean compression, long blockId, TStore tStore, RStore rStore) {
    if (compression) {
      rawDAO.putCompressed(tableId, blockId,
          tStore.getMapping(), tStore.getRawData(),
          rStore.getMappingInt(), rStore.getRawDataInt(),
          rStore.getMappingLong(), rStore.getRawDataLong(),
          rStore.getMappingFloat(), rStore.getRawDataFloat(),
          rStore.getMappingDouble(), rStore.getRawDataDouble(),
          rStore.getMappingString(), rStore.getRawDataString());

      return;
    }

    this.rawDAO.putLong(tableId, blockId, tStore.mappingToArray(), tStore.dataToArray());

    if (rStore.getInitialCapacityInt() > 0)
      this.rawDAO.putInt(tableId, blockId,
          rStore.getMappingInt().keySet().stream().mapToInt(i -> i).toArray(), getArrayInt(rStore.getRawDataInt()));
    if (rStore.getInitialCapacityLong() > 0)
      this.rawDAO.putLong(tableId, blockId,
          rStore.getMappingLong().keySet().stream().mapToInt(i -> i).toArray(), getArrayLong(rStore.getRawDataLong()));
    if (rStore.getInitialCapacityFloat() > 0)
      this.rawDAO.putFloat(tableId, blockId,
          rStore.getMappingFloat().keySet().stream().mapToInt(i -> i).toArray(), getArrayFloat(rStore.getRawDataFloat()));
    if (rStore.getInitialCapacityDouble() > 0)
      this.rawDAO.putDouble(tableId, blockId,
          rStore.getMappingDouble().keySet().stream().mapToInt(i -> i).toArray(), getArrayDouble(rStore.getRawDataDouble()));
    if (rStore.getInitialCapacityString() > 0)
      this.rawDAO.putString(tableId, blockId,
          rStore.getMappingString().keySet().stream().mapToInt(i -> i).toArray(), getArrayString(rStore.getRawDataString()));
  }

  private void storeDataLocal(byte tableId, boolean compression, long blockId,
      List<CProfile> cProfiles, TStore tStore, Map<Integer, HEntry> histograms) {

    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    histograms.forEach((colId, value) -> {
      int sizeOfRaw = tStore.size();
      int sizeOfHist = value.getValue().size();

      if (sizeOfHist < (sizeOfRaw / 2)) {
        colIdSTypeMap.put(colId, SType.HISTOGRAM);
      } else {
        if (value.getValue().stream().distinct().count() > 255) {
          colIdSTypeMap.put(colId, SType.RAW);
        } else {
          colIdSTypeMap.put(colId, SType.ENUM);
        }
      }
    });

    /* Raw */
    RStore rStore = new RStore(converter, cProfiles, colIdSTypeMap);

    /* Enums */
    EStore eStore = new EStore(cProfiles, colIdSTypeMap);

    cProfiles.forEach(cProfile -> {
          int colId = cProfile.getColId();

          if (SType.ENUM.equals(colIdSTypeMap.get(colId))) {
            int iMapping = eStore.getMapping().get(colId);

            List<Integer> indexList = histograms.get(colId).getIndex();
            List<Integer> valueList = histograms.get(colId).getValue();

            for (int i = 0; i < indexList.size(); i++) {
              int curIndex = indexList.get(i);
              int nextIndex;

              int curValue = valueList.get(i);

              if (indexList.size() != i + 1) {
                nextIndex = indexList.get(i + 1);

                for (int j = curIndex; j < nextIndex; j++) {
                  try {
                    eStore.add(iMapping, j, curValue);
                  } catch (EnumByteExceedException e) {
                    throw new RuntimeException(e);
                  }
                }
              } else {
                nextIndex = tStore.size() - 1;

                for (int j = curIndex; j <= nextIndex; j++) {
                  try {
                    eStore.add(iMapping, j, curValue);
                  } catch (EnumByteExceedException e) {
                    throw new RuntimeException(e);
                  }
                }
              }
            }
          } else if (SType.RAW.equals(colIdSTypeMap.get(cProfile.getColId()))) {
            List<Integer> indexList = histograms.get(colId).getIndex();
            List<Integer> valueList = histograms.get(colId).getValue();

            for (int i = 0; i < indexList.size(); i++) {
              int curIndex = indexList.get(i);
              int nextIndex;

              int curValue = valueList.get(i);

              if (indexList.size() != i + 1) {
                nextIndex = indexList.get(i + 1);

                for (int j = curIndex; j < nextIndex; j++) {
                  rStore.add(Mapper.isCType(cProfile), cProfile, j, curValue);
                }
              } else {
                nextIndex = tStore.size() - 1;

                for (int j = curIndex; j <= nextIndex; j++) {
                  rStore.add(Mapper.isCType(cProfile), cProfile, j, curValue);
                }
              }
            }
          }
    });

    /* Store metadata */
    this.storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    /* Store raw data */
    this.storeRaw(tableId, compression, blockId, tStore, rStore);

    /* Store enum data */
    this.storeEnum(tableId, compression, blockId, eStore);

    /* Store histogram data */
    colIdSTypeMap.entrySet().stream()
        .filter(f -> !SType.HISTOGRAM.equals(f.getValue()))
        .forEach(obj -> histograms.remove(obj.getKey()));

    this.storeHistogramsEntry(tableId, compression, blockId, histograms);
  }

  @Override
  public void putDataJdbcBatch(String tableName, ResultSet resultSet, Integer fBaseBatchSize) {
    byte tableId = getTableId(tableName, metaModel);
    boolean compression = getTableCompression(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    int colCount = cProfiles.size();
    int rowCount = fBaseBatchSize;

    /* Timestamp */
    int colRawDataTimeStampCount = 1;
    long[][] rawDataTimestamp = new long[colRawDataTimeStampCount][rowCount];
    List<Integer> rawDataTimeStampMapping = new ArrayList<>(colRawDataTimeStampCount);
    fillTimestampMapping(cProfiles, rawDataTimeStampMapping);

    /* Int */
    int colRawDataIntCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isInt);
    int[][] rawDataInt = new int[colRawDataIntCount][rowCount];
    List<Integer> rawDataIntMapping = new ArrayList<>(colRawDataIntCount);
    fillMappingRaw(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isLong);
    long[][] rawDataLong = new long[colRawDataLongCount][rowCount];
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMappingRaw(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isFloat);
    float[][] rawDataFloat = new float[colRawDataFloatCount][rowCount];
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillMappingRaw(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isDouble);
    double[][] rawDataDouble = new double[colRawDataDoubleCount][rowCount];
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMappingRaw(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isString);
    String[][] rawDataString = new String[colRawDataStringCount][rowCount];
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMappingRaw(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

    /* Enums */
    int colRawDataEnumCount = (int) cProfiles.stream().filter(isEnum).count();
    byte[][] rawDataEnum = new byte[colRawDataEnumCount][rowCount];
    List<Integer> rawDataEnumMapping = new ArrayList<>(colRawDataEnumCount);
    List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

    // Fill histogram data
    Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms = new HashMap<>(colCount);
    cProfiles.stream().filter(isHistogram).forEach(e -> mapOfHistograms.put(e.getColId(),
        new CachedLastLinkedHashMap<>()));

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        // Reinitialize
        if (iR == fBaseBatchSize) {
          long blockId = rawDataTimestamp[0][0];

          /* Store metadata */
          this.storeMetadata(tableId, blockId, cProfiles);

          this.storeHistograms(tableId, compression, blockId, mapOfHistograms);

          this.storeData(tableId, compression, blockId,
              rawDataTimeStampMapping, rawDataTimestamp,
              colRawDataIntCount, rawDataIntMapping, rawDataInt,
              colRawDataLongCount, rawDataLongMapping, rawDataLong,
              colRawDataFloatCount, rawDataFloatMapping, rawDataFloat,
              colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
              colRawDataStringCount, rawDataStringMapping, rawDataString,
              colRawDataEnumCount, rawDataEnumMapping, rawDataEnum, rawDataEnumEColumn);

          iRow.set(0);
          iR = iRow.getAndAdd(1);

          /* Timestamp */
          rawDataTimestamp = new long[colRawDataTimeStampCount][rowCount];
          rawDataTimeStampMapping = new ArrayList<>(colRawDataTimeStampCount);
          fillTimestampMapping(cProfiles, rawDataTimeStampMapping);

          /* Int */
          rawDataInt = new int[colRawDataIntCount][rowCount];
          rawDataIntMapping = new ArrayList<>(colRawDataIntCount);
          fillMappingRaw(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

          /* Long */
          rawDataLong = new long[colRawDataLongCount][rowCount];
          rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
          fillMappingRaw(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

          /* Float */
          rawDataFloat = new float[colRawDataFloatCount][rowCount];
          rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
          fillMappingRaw(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

          /* Double */
          rawDataDouble = new double[colRawDataDoubleCount][rowCount];
          rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
          fillMappingRaw(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

          /* String */
          rawDataString = new String[colRawDataStringCount][rowCount];
          rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
          fillMappingRaw(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

          /* Enums */
          rawDataEnum = new byte[colRawDataEnumCount][rowCount];
          rawDataEnumMapping = new ArrayList<>(colRawDataEnumCount);
          rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
          fillEnumMapping(cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

          // Fill histogram data
          mapOfHistograms.replaceAll((k,v) -> new CachedLastLinkedHashMap<>());
        }

        for (int iC = 0; iC < colCount; iC++) {

          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW) {
                rawDataTimestamp[rawDataTimeStampMapping.indexOf(iC)][iR] = this.converter.getKeyValue(currObject, cProfile);
              }
            }

            // Fill raw data
            if (csType.getSType() == SType.RAW) {
              if (!csType.isTimeStamp()) {
                if (CType.INT == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataInt[rawDataIntMapping.indexOf(iC)][iR] =
                      Mapper.convertRawToInt(currObject, cProfile);
                } else if (CType.LONG == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataLong[rawDataLongMapping.indexOf(iC)][iR] =
                      Mapper.convertRawToLong(currObject, cProfile);
                } else if (CType.FLOAT == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataFloat[rawDataFloatMapping.indexOf(iC)][iR] =
                      Mapper.convertRawToFloat(currObject, cProfile);
                } else if (CType.DOUBLE == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataDouble[rawDataDoubleMapping.indexOf(iC)][iR] =
                      Mapper.convertRawToDouble(currObject, cProfile);
                } else if (CType.STRING == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataString[rawDataStringMapping.indexOf(iC)][iR] =
                      Mapper.convertRawToString(currObject, cProfile);
                }
              }
            }

            // Fill enum data
            if (csType.getSType() == SType.ENUM) {
              int valueInt = this.converter.convertRawToInt(currObject, cProfile);

              try {
                rawDataEnum[rawDataEnumMapping.indexOf(iC)][iR] =
                    EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt);
              } catch (EnumByteExceedException e) {
                log.error(e.getMessage());
              }
            }

            // Fill histogram data
            if (csType.getSType() == SType.HISTOGRAM) {
              int currInt = this.converter.convertRawToInt(currObject, cProfile);

              if (iR != 0) {
                Integer prevObject = mapOfHistograms.get(cProfile.getColId()).getLast();
                int prevInt = prevObject == null ?  Integer.MIN_VALUE : prevObject;

                if (prevInt != currInt) {
                  mapOfHistograms.get(cProfile.getColId()).put(iR, currInt);
                }
              } else {
                mapOfHistograms.get(cProfile.getColId()).put(iR, currInt);
              }
            }

          } catch (SQLException e) {
            log.catching(e);
            throw new RuntimeException(e);
          }
        }

      }

      if (iRow.get() <= fBaseBatchSize) {
        long blockId = rawDataTimestamp[0][0];

        /* Store metadata */
        this.storeMetadata(tableId, blockId, cProfiles);

        this.storeHistograms(tableId, compression, blockId,  mapOfHistograms);

        int row = iRow.get();

        this.storeData(tableId, compression, blockId,
            rawDataTimeStampMapping, copyOfLong(rawDataTimestamp, row),
            colRawDataIntCount, rawDataIntMapping, copyOfInt(rawDataInt, row),
            colRawDataLongCount, rawDataLongMapping, copyOfLong(rawDataLong, row),
            colRawDataFloatCount, rawDataFloatMapping, copyOfFloat(rawDataFloat, row),
            colRawDataDoubleCount, rawDataDoubleMapping, copyOfDouble(rawDataDouble, row),
            colRawDataStringCount, rawDataStringMapping, copyOfString(rawDataString, row),
            colRawDataEnumCount, rawDataEnumMapping, copyOfByte(rawDataEnum, row), rawDataEnumEColumn);

        log.info("Final flush for iRow: " + iRow.get());
      }

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void putDataCsvBatch(String tableName, String fileName, String csvSplitBy, Integer fBaseBatchSize) {
    byte tableId = getTableId(tableName, metaModel);
    boolean compression = getTableCompression(tableName, metaModel);
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    final AtomicLong counter = new AtomicLong(rawDAO.getLastBlockId(tableId));

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isRaw, isLong);
    List<List<Long>> rawDataLong = new ArrayList<>(colRawDataLongCount);
    fillArrayList(rawDataLong, colRawDataLongCount);
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMappingRaw(cProfiles, rawDataLongMapping, isRaw, isLong);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isRaw, isDouble);
    List<List<Double>> rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
    fillArrayList(rawDataDouble, colRawDataDoubleCount);
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMappingRaw(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isRaw, isString);
    List<List<String>> rawDataString = new ArrayList<>(colRawDataStringCount);
    fillArrayList(rawDataString, colRawDataStringCount);
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMappingRaw(cProfiles, rawDataStringMapping, isRaw, isString);

    String line = "";
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      line = br.readLine();
      String[] headers = line.split(csvSplitBy);
      log.info("Header = " + Arrays.toString(headers));

      final AtomicInteger iRow = new AtomicInteger(0);
      while ((line = br.readLine()) != null) {

        int iR = iRow.getAndAdd(1);

        // Reinitialize
        if (iR == fBaseBatchSize) {
          long blockId = counter.getAndAdd(1);

          /* Store metadata */
          this.storeMetadata(tableId, blockId, cProfiles);

          this.storeData(tableId, compression, blockId,
              colRawDataLongCount, rawDataLongMapping, rawDataLong,
              colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
              colRawDataStringCount, rawDataStringMapping, rawDataString);

          iRow.set(0);
          iRow.getAndAdd(1);

          /* Long */
          rawDataLong = new ArrayList<>(colRawDataLongCount);
          fillArrayList(rawDataLong, colRawDataLongCount);
          rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
          fillMappingRaw(cProfiles, rawDataLongMapping, isRaw, isLong);

          /* Double */
          rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
          fillArrayList(rawDataDouble, colRawDataDoubleCount);
          rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
          fillMappingRaw(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

          /* String */
          rawDataString = new ArrayList<>(colRawDataStringCount);
          fillArrayList(rawDataString, colRawDataStringCount);
          rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
          fillMappingRaw(cProfiles, rawDataStringMapping, isRaw, isString);
        }

        String[] data = line.split(csvSplitBy);

        for (int iC = 0; iC < headers.length; iC++) {
          String header = headers[iC];
          String colData = data[iC];

          Optional<CProfile> optionalCProfile = cProfiles.stream()
              .filter(f -> f.getColName().equals(header))
              .findAny();

          // Fill raw data
          if (optionalCProfile.isPresent()) {
            CProfile cProfile = optionalCProfile.get();
            if (cProfile.getCsType().getSType() == SType.RAW) {
              if (CType.LONG == Mapper.isCType(cProfile)) {
                rawDataLong.get(rawDataLongMapping.indexOf(iC)).add(Long.valueOf(colData));
              } else if (CType.DOUBLE == Mapper.isCType(cProfile)) {
                rawDataDouble.get(rawDataDoubleMapping.indexOf(iC)).add(Double.valueOf(colData));
              } else if (CType.STRING == Mapper.isCType(cProfile)) {
                rawDataString.get(rawDataStringMapping.indexOf(iC)).add(colData);
              }
            }
          }
        }
      }

      if (iRow.get() <= fBaseBatchSize) {
        long blockId = counter.getAndAdd(1);

        /* Store metadata */
        this.storeMetadata(tableId, blockId, cProfiles);

        this.storeData(tableId, compression, blockId,
            colRawDataLongCount, rawDataLongMapping, rawDataLong,
            colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
            colRawDataStringCount, rawDataStringMapping, rawDataString);

        log.info("Final flush for iRow: " + iRow.get());
      }

    } catch (IOException e) {
      log.catching(e);
    }

  }

  private void fillEnumMapping(List<CProfile> cProfiles, List<Integer> mapping,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn) {

    final AtomicInteger iRawDataEnumMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isEnum)
        .forEach(cProfile -> {
          int var = iRawDataEnumMapping.getAndAdd(1);
          mapping.add(var, cProfile.getColId());
          rawDataEnumEColumn.add(var, new CachedLastLinkedHashMap<>());
        });
  }

  private void storeMetadata(byte tableId, long blockId, List<CProfile> cProfiles) {
    List<Byte> rawCTypeKeys = new ArrayList<>();
    List<Integer> rawColIds = new ArrayList<>();
    List<Integer> enumColIds = new ArrayList<>();
    List<Integer> histogramColIds = new ArrayList<>();

    cProfiles.forEach(cProfile -> {
      if (SType.RAW.equals(cProfile.getCsType().getSType())) {
        rawCTypeKeys.add(cProfile.getCsType().getCType().getKey());
        rawColIds.add(cProfile.getColId());
      } else if (SType.ENUM.equals(cProfile.getCsType().getSType())) {
        enumColIds.add(cProfile.getColId());
      } else if (SType.HISTOGRAM.equals(cProfile.getCsType().getSType())) {
        histogramColIds.add(cProfile.getColId());
      }
    });

    this.rawDAO.putMetadata(tableId, blockId, getByteFromList(rawCTypeKeys),
        rawColIds.stream().mapToInt(j -> j).toArray(),
        enumColIds.stream().mapToInt(j -> j).toArray(),
        histogramColIds.stream().mapToInt(j -> j).toArray());
  }

  private void storeMetadataLocal(byte tableId, long blockId, List<CProfile> cProfiles,
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

  private void storeHistograms(byte tableID, boolean compression, long blockId, Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms) {
    mapOfHistograms.forEach((colId, hData) -> {
      if (!hData.isEmpty()) {
        if (compression) {
          this.histogramDAO.putCompressed(tableID, blockId, colId, getArrayFromMap(hData));
        } else {
          this.histogramDAO.put(tableID, blockId, colId, getArrayFromMap(hData));
        }
      }
    });
  }

  private void storeHistogramsEntry(byte tableID, boolean compression, long blockId,
      Map<Integer, HEntry> histograms) {

    histograms.forEach((colId, hEntry) -> {
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

  private void storeData(byte tableId, boolean compression, long blockId,
      List<Integer> rawDataTimeStampMapping, long[][] rawDataTimestamp,
      int colRawDataIntCount, List<Integer> rawDataIntMapping, int[][] rawDataInt,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, long[][] rawDataLong,
      int colRawDataFloatCount, List<Integer> rawDataFloatMapping, float[][] rawDataFloat,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, double[][] rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, String[][] rawDataString,
      int colRawDataEnumCount, List<Integer> rawDataEnumMapping, byte[][] rawDataEnum,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn) {

    if (compression) {
      try {
        rawDAO.putCompressed(tableId, blockId,
            rawDataTimeStampMapping, Arrays.stream(rawDataTimestamp)
                .map(ia -> Arrays.stream(ia)
                    .boxed()
                    .collect(Collectors.toList()))
                .collect(Collectors.toList()),
            rawDataIntMapping, Arrays.stream(rawDataInt)
                .map(ia -> Arrays.stream(ia)
                    .boxed()
                    .collect(Collectors.toList()))
                .collect(Collectors.toList()),
            rawDataLongMapping, Arrays.stream(rawDataLong)
                .map(ia -> Arrays.stream(ia)
                    .boxed()
                    .collect(Collectors.toList()))
                .collect(Collectors.toList()),
            rawDataFloatMapping, convert2DFloatArrayToList(rawDataFloat),
            rawDataDoubleMapping, Arrays.stream(rawDataDouble)
                .map(ia -> Arrays.stream(ia)
                    .boxed()
                    .collect(Collectors.toList()))
                .collect(Collectors.toList()),
            rawDataStringMapping, Arrays.stream(rawDataString)
                .map(ia -> Arrays.stream(ia)
                    .collect(Collectors.toList()))
                .collect(Collectors.toList()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      /* Store enum data */
      if (colRawDataEnumCount > 0) {
        this.storeEnum(tableId, compression, blockId, colRawDataEnumCount, rawDataEnumMapping, rawDataEnumEColumn, rawDataEnum);
      }

      return;
    }

    /* Store timestamp raw data entity */
    this.rawDAO.putLong(tableId, blockId, rawDataTimeStampMapping.stream().mapToInt(i -> i).toArray(), rawDataTimestamp);

    /* Store raw data entity */
    if (colRawDataIntCount > 0)
      this.rawDAO.putInt(tableId, blockId, rawDataIntMapping.stream().mapToInt(i -> i).toArray(), rawDataInt);
    if (colRawDataLongCount > 0)
      this.rawDAO.putLong(tableId, blockId, rawDataLongMapping.stream().mapToInt(i -> i).toArray(), rawDataLong);
    if (colRawDataFloatCount > 0)
      this.rawDAO.putFloat(tableId, blockId, rawDataFloatMapping.stream().mapToInt(i -> i).toArray(), rawDataFloat);
    if (colRawDataDoubleCount > 0)
      this.rawDAO.putDouble(tableId, blockId, rawDataDoubleMapping.stream().mapToInt(i -> i).toArray(), rawDataDouble);
    if (colRawDataStringCount > 0)
      this.rawDAO.putString(tableId, blockId, rawDataStringMapping.stream().mapToInt(i -> i).toArray(), rawDataString);

    /* Store enum data */
    if (colRawDataEnumCount > 0) {
      this.storeEnum(tableId, compression, blockId, colRawDataEnumCount, rawDataEnumMapping, rawDataEnumEColumn, rawDataEnum);
    }
  }

  private void storeEnum(byte tableId, boolean compression, long blockId, EStore eStore) {

    if (eStore.getInitialCapacity() > 0) {
      eStore.getMapping().forEach((colId, iMapping) -> {

        int[] values = new int[eStore.getRawDataEColumn().get(iMapping).size()];

        AtomicInteger counter = new AtomicInteger(0);
        eStore.getRawDataEColumn().get(iMapping)
            .forEach((enumKey, enumValue) -> values[counter.getAndAdd(1)] = enumKey);

        try {
          this.enumDAO.putEColumn(tableId, blockId, colId, values,
              getByteFromList(eStore.getRawData().get(iMapping)), compression);
        } catch (IOException e) {
          log.catching(e);
          throw new RuntimeException(e);
        }
      });
    }

  }

  private void storeEnum(byte tableId, boolean compression, long blockId,
      int colRawDataEnumCount, List<Integer> rawDataEnumMapping,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn, byte[][] rawDataEnum) {

    if (colRawDataEnumCount > 0) {
      for (int i = 0; i < rawDataEnumMapping.size(); i++) {
        int colId = rawDataEnumMapping.get(i);

        int[] values = new int[rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(colId)).size()];

        AtomicInteger counter = new AtomicInteger(0);
        rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(colId))
            .forEach((enumKey, enumValue) -> values[counter.getAndAdd(1)] = enumKey);

        try {
          this.enumDAO.putEColumn(tableId, blockId, colId, values, rawDataEnum[i], compression);
        } catch (IOException e) {
          log.catching(e);
          throw new RuntimeException(e);
        }
      }
    }

  }

  private void storeData(byte tableId, boolean compression, long blockId,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, List<List<String>> rawDataString) {

    /* Store raw data entity */
    if (compression) {
      try {
        rawDAO.putCompressed(tableId, blockId,
            Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(),
            rawDataLongMapping, rawDataLong,
            Collections.emptyList(), Collections.emptyList(),
            rawDataDoubleMapping, rawDataDouble,
            rawDataStringMapping, rawDataString);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return;
    }

    if (colRawDataLongCount > 0)
      this.rawDAO.putLong(tableId, blockId, rawDataLongMapping.stream().mapToInt(i -> i).toArray(), getArrayLong(rawDataLong));
    if (colRawDataDoubleCount > 0)
      this.rawDAO.putDouble(tableId, blockId, rawDataDoubleMapping.stream().mapToInt(i -> i).toArray(), getArrayDouble(rawDataDouble));
    if (colRawDataStringCount > 0)
      this.rawDAO.putString(tableId, blockId, rawDataStringMapping.stream().mapToInt(i -> i).toArray(), getArrayString(rawDataString));

  }

}
