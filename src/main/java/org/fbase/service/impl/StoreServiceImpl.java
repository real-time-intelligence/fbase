package org.fbase.service.impl;

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
import java.util.function.Predicate;
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

  Predicate<CProfile> isNotTimestamp = Predicate.not(f -> f.getCsType().isTimeStamp());
  Predicate<CProfile> isRaw = Predicate.not(f -> f.getCsType().getSType() != SType.RAW);
  Predicate<CProfile> isEnum = Predicate.not(f -> f.getCsType().getSType() != SType.ENUM);
  Predicate<CProfile> isHistogram = Predicate.not(f -> f.getCsType().getSType() != SType.HISTOGRAM);
  Predicate<CProfile> isInt = Predicate.not(f -> Mapper.isCType(f) != CType.INT);
  Predicate<CProfile> isLong = Predicate.not(f -> Mapper.isCType(f) != CType.LONG);
  Predicate<CProfile> isFloat = Predicate.not(f -> Mapper.isCType(f) != CType.FLOAT);
  Predicate<CProfile> isDouble = Predicate.not(f -> Mapper.isCType(f) != CType.DOUBLE);
  Predicate<CProfile> isString = Predicate.not(f -> Mapper.isCType(f) != CType.STRING);

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
    fillMapping(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isLong);
    long[][] rawDataLong = new long[colRawDataLongCount][rowCount];
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMapping(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isFloat);
    float[][] rawDataFloat = new float[colRawDataFloatCount][rowCount];
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillMapping(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isDouble);
    double[][] rawDataDouble = new double[colRawDataDoubleCount][rowCount];
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMapping(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isString);
    String[][] rawDataString = new String[colRawDataStringCount][rowCount];
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMapping(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

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

    /* Timestamp */
    List<List<Long>> rawDataTimestamp = new ArrayList<>(1);
    fillArrayList(rawDataTimestamp, 1);
    List<Integer> rawDataTimeStampMapping = new ArrayList<>(1);
    fillTimestampMapping(cProfiles, rawDataTimeStampMapping);

    /* Int */
    int colRawDataIntCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isInt);
    List<List<Integer>> rawDataInt = new ArrayList<>(colRawDataIntCount);
    fillArrayList(rawDataInt, colRawDataIntCount);
    List<Integer> rawDataIntMapping = new ArrayList<>(colRawDataIntCount);
    fillMapping(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isLong);
    List<List<Long>> rawDataLong = new ArrayList<>(colRawDataLongCount);
    fillArrayList(rawDataLong, colRawDataLongCount);
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMapping(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isFloat);
    List<List<Float>> rawDataFloat = new ArrayList<>(colRawDataFloatCount);
    fillArrayList(rawDataFloat, colRawDataFloatCount);
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillMapping(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isDouble);
    List<List<Double>> rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
    fillArrayList(rawDataDouble, colRawDataDoubleCount);
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMapping(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isString);
    List<List<String>> rawDataString = new ArrayList<>(colRawDataStringCount);
    fillArrayList(rawDataString, colRawDataStringCount);
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMapping(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

    /* Enums */
    int colRawDataEnumCount = (int) cProfiles.stream().filter(isEnum).count();
    List<List<Byte>> rawDataEnum = new ArrayList<>(colRawDataEnumCount);
    fillArrayList(rawDataEnum, colRawDataEnumCount);

    List<Integer> rawDataEnumMapping = new ArrayList<>(colRawDataEnumCount);
    List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

    // Fill histogram data
    Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms = new HashMap<>();
    cProfiles.stream().filter(isHistogram).forEach(e -> mapOfHistograms.put(e.getColId(), new CachedLastLinkedHashMap<>()));

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        IntStream iColumn = IntStream.range(0, colCount);
        iColumn.forEach(iC -> {

          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW){
                rawDataTimestamp.get(rawDataTimeStampMapping.indexOf(iC)).add(iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            // Fill raw data
            if (csType.getSType() == SType.RAW) {
              if (!csType.isTimeStamp()) {
                if (CType.INT == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataInt.get(rawDataIntMapping.indexOf(iC)).add(iR, Mapper.convertRawToInt(currObject, cProfile));
                } else if (CType.LONG == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataLong.get(rawDataLongMapping.indexOf(iC)).add(iR, Mapper.convertRawToLong(currObject, cProfile));
                } else if (CType.FLOAT == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataFloat.get(rawDataFloatMapping.indexOf(iC)).add(iR, Mapper.convertRawToFloat(currObject, cProfile));
                } else if (CType.DOUBLE == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataDouble.get(rawDataDoubleMapping.indexOf(iC)).add(iR, Mapper.convertRawToDouble(currObject, cProfile));
                } else if (CType.STRING == Mapper.isCType(cProfiles.get(iC))) {
                  rawDataString.get(rawDataStringMapping.indexOf(iC)).add(iR, Mapper.convertRawToString(currObject, cProfile));
                }
              }
            }

            // Fill enum data
            if (csType.getSType() == SType.ENUM) {
              int valueInt = this.converter.convertRawToInt(currObject, cProfile);

              try {
                rawDataEnum.get(rawDataEnumMapping.indexOf(iC)).add(iR,
                    EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt));
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
        });
      }

      if (rawDataTimestamp.get(0).size() == 0) {
        return -1;
      }

      long blockId = rawDataTimestamp.get(0).get(0);

      /* Store metadata */
      this.storeMetadata(tableId, blockId, cProfiles);

      this.storeHistograms(tableId, compression, blockId, mapOfHistograms);

      this.storeData(tableId, compression, blockId,
          rawDataTimeStampMapping, getArrayLong(rawDataTimestamp),
          colRawDataIntCount, rawDataIntMapping, getArrayInt(rawDataInt),
          colRawDataLongCount, rawDataLongMapping, getArrayLong(rawDataLong),
          colRawDataFloatCount, rawDataFloatMapping, getArrayFloat(rawDataFloat),
          colRawDataDoubleCount, rawDataDoubleMapping, getArrayDouble(rawDataDouble),
          colRawDataStringCount, rawDataStringMapping, getArrayString(rawDataString),
          colRawDataEnumCount, rawDataEnumMapping, getArrayByte(rawDataEnum), rawDataEnumEColumn);

      return rawDataTimestamp.get(0).get(rawDataTimestamp.get(0).size() - 1);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  // TODO WIP
  private long putDataJdbcLocal(String tableName, ResultSet resultSet) {
    byte tableId = getTableId(tableName, metaModel);
    boolean compression = getTableCompression(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);
    int colCount = cProfiles.size();

    /* Timestamp */
    List<List<Long>> rawDataTimestamp = new ArrayList<>(1);
    fillArrayList(rawDataTimestamp, 1);
    List<Integer> rawDataTimeStampMapping = new ArrayList<>(1);
    fillTimestampMapping(cProfiles, rawDataTimeStampMapping);

    /* Int */
    int colRawDataIntCount = Mapper.getRawCTypeColumnCount(cProfiles, isNotTimestamp, isInt);
    List<List<Integer>> rawDataInt = new ArrayList<>(colRawDataIntCount);
    fillArrayList(rawDataInt, colRawDataIntCount);
    List<Integer> rawDataIntMapping = new ArrayList<>(colRawDataIntCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataIntMapping, isNotTimestamp, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getRawCTypeColumnCount(cProfiles, isNotTimestamp, isLong);
    List<List<Long>> rawDataLong = new ArrayList<>(colRawDataLongCount);
    fillArrayList(rawDataLong, colRawDataLongCount);
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataLongMapping, isNotTimestamp, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getRawCTypeColumnCount(cProfiles, isNotTimestamp, isFloat);
    List<List<Float>> rawDataFloat = new ArrayList<>(colRawDataFloatCount);
    fillArrayList(rawDataFloat, colRawDataFloatCount);
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataFloatMapping, isNotTimestamp, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getRawCTypeColumnCount(cProfiles, isNotTimestamp, isDouble);
    List<List<Double>> rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
    fillArrayList(rawDataDouble, colRawDataDoubleCount);
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataDoubleMapping, isNotTimestamp, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getRawCTypeColumnCount(cProfiles, isNotTimestamp, isString);
    List<List<String>> rawDataString = new ArrayList<>(colRawDataStringCount);
    fillArrayList(rawDataString, colRawDataStringCount);
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataStringMapping, isNotTimestamp, isString);

    /* Enums */
    int colRawDataEnumCount = colCount - 1;
    List<List<Byte>> rawDataEnum = new ArrayList<>(colRawDataEnumCount);
    fillArrayList(rawDataEnum, colRawDataEnumCount);

    List<Integer> rawDataEnumMapping = new ArrayList<>(colRawDataEnumCount);
    List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillAllEnumMapping(cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

    // Fill histogram data
    Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms = new HashMap<>();
    cProfiles.stream().filter(isNotTimestamp).forEach(e -> mapOfHistograms.put(e.getColId(), new CachedLastLinkedHashMap<>()));

    try {
      final AtomicInteger iRow = new AtomicInteger(0);

      while (resultSet.next()) {
        int iR = iRow.getAndAdd(1);

        IntStream iColumn = IntStream.range(0, colCount);
        iColumn.forEach(iC -> {

          try {
            CProfile cProfile = cProfiles.get(iC);
            CSType csType = cProfile.getCsType();

            Object currObject = resultSet.getObject(cProfile.getColIdSql());

            // Fill timestamps
            if (csType.isTimeStamp()) {
              if (csType.getSType() == SType.RAW){
                rawDataTimestamp.get(rawDataTimeStampMapping.indexOf(iC)).add(iR, this.converter.getKeyValue(currObject, cProfile));
              }
            }

            if (!csType.isTimeStamp()) {
              // Fill raw data
              if (CType.INT == Mapper.isCType(cProfiles.get(iC))) {
                rawDataInt.get(rawDataIntMapping.indexOf(iC)).add(iR, Mapper.convertRawToInt(currObject, cProfile));
              } else if (CType.LONG == Mapper.isCType(cProfiles.get(iC))) {
                rawDataLong.get(rawDataLongMapping.indexOf(iC)).add(iR, Mapper.convertRawToLong(currObject, cProfile));
              } else if (CType.FLOAT == Mapper.isCType(cProfiles.get(iC))) {
                rawDataFloat.get(rawDataFloatMapping.indexOf(iC)).add(iR, Mapper.convertRawToFloat(currObject, cProfile));
              } else if (CType.DOUBLE == Mapper.isCType(cProfiles.get(iC))) {
                rawDataDouble.get(rawDataDoubleMapping.indexOf(iC)).add(iR, Mapper.convertRawToDouble(currObject, cProfile));
              } else if (CType.STRING == Mapper.isCType(cProfiles.get(iC))) {
                rawDataString.get(rawDataStringMapping.indexOf(iC)).add(iR, Mapper.convertRawToString(currObject, cProfile));
              }

              // Fill enum data
              // TODO WIP

              // Fill histogram data
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
        });
      }

      if (rawDataTimestamp.get(0).size() == 0) {
        return -1;
      }

      long blockId = rawDataTimestamp.get(0).get(0);

      /* Store data and metadata */
      this.storeDataLocal(tableId, compression, blockId, cProfiles,
          rawDataTimeStampMapping, rawDataTimestamp,
          rawDataIntMapping, rawDataInt,
          rawDataLongMapping, rawDataLong,
          rawDataFloatMapping, rawDataFloat,
          rawDataDoubleMapping, rawDataDouble,
          rawDataStringMapping, rawDataString,
          mapOfHistograms,
          colRawDataEnumCount, rawDataEnumMapping, rawDataEnum, rawDataEnumEColumn);

      return rawDataTimestamp.get(0).get(rawDataTimestamp.get(0).size() - 1);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void storeDataLocal(byte tableId, boolean compression, long blockId, List<CProfile> cProfiles,
      List<Integer> rawDataTimeStampMapping, List<List<Long>> rawDataTimestamp,
      List<Integer> rawDataIntMapping, List<List<Integer>> rawDataInt,
      List<Integer> rawDataLongMapping, List<List<Long>> rawDataLong,
      List<Integer> rawDataFloatMapping, List<List<Float>> rawDataFloat,
      List<Integer> rawDataDoubleMapping, List<List<Double>> rawDataDouble,
      List<Integer> rawDataStringMapping, List<List<String>> rawDataString,
      Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms,
      int colRawDataEnumCount, List<Integer> rawDataEnumMapping, List<List<Byte>> rawDataEnum,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn) {

    Map<Integer, CProfile> cProfileMap =
        cProfiles.stream().filter(isNotTimestamp)
            .collect(Collectors.toMap(CProfile::getColId, cProfile -> cProfile));
        
    Map<Integer, SType> colIdSTypeMap = new HashMap<>();

    Map<Integer, Integer> rawDataIntMap = mappingIndexToColId(rawDataIntMapping);
    Map<Integer, Integer> rawDataLongMap = mappingIndexToColId(rawDataLongMapping);
    Map<Integer, Integer> rawDataFloatMap = mappingIndexToColId(rawDataFloatMapping);
    Map<Integer, Integer> rawDataDoubleMap = mappingIndexToColId(rawDataDoubleMapping);
    Map<Integer, Integer> rawDataStringMap = mappingIndexToColId(rawDataStringMapping);

    mapOfHistograms.forEach((key, value) -> {
      CProfile cProfile = cProfileMap.get(key);

      int sizeOfHist = value.size();
      
      int sizeOfRaw = 0;

      if (CType.INT.equals(Mapper.isCType(cProfile))) {
        sizeOfRaw = rawDataInt.get(rawDataIntMap.get(key)).size();
      } else if (CType.LONG.equals(Mapper.isCType(cProfile))) {
        sizeOfRaw = rawDataLong.get(rawDataLongMap.get(key)).size();
      } else if (CType.FLOAT.equals(Mapper.isCType(cProfile))) {
        sizeOfRaw = rawDataFloat.get(rawDataFloatMap.get(key)).size();
      } else if (CType.DOUBLE.equals(Mapper.isCType(cProfile))) {
        sizeOfRaw = rawDataDouble.get(rawDataDoubleMap.get(key)).size();
      } else if (CType.STRING.equals(Mapper.isCType(cProfile))) {
        sizeOfRaw = rawDataString.get(rawDataStringMap.get(key)).size();
      }

      if (sizeOfHist < (sizeOfRaw / 2)) {
        colIdSTypeMap.put(key, SType.HISTOGRAM);
      } else {
        colIdSTypeMap.put(key, SType.RAW);
      }

    });

    this.storeMetadataLocal(tableId, blockId, cProfiles, colIdSTypeMap);

    /* Int */
    int colRawDataIntLocalCount = Mapper.getColumnCountLocal(cProfiles, colIdSTypeMap, isNotTimestamp, isInt);
    List<List<Integer>> rawDataIntLocal = new ArrayList<>(colRawDataIntLocalCount);
    fillArrayList(rawDataIntLocal, colRawDataIntLocalCount);
    List<Integer> rawDataIntMappingLocal = new ArrayList<>(colRawDataIntLocalCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataIntMappingLocal, isNotTimestamp, isInt);

    /* Long */
    int colRawDataLongLocalCount = Mapper.getColumnCountLocal(cProfiles, colIdSTypeMap, isNotTimestamp, isLong);
    List<List<Long>> rawDataLongLocal = new ArrayList<>(colRawDataLongLocalCount);
    fillArrayList(rawDataLongLocal, colRawDataLongLocalCount);
    List<Integer> rawDataLongMappingLocal = new ArrayList<>(colRawDataLongLocalCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataLongMappingLocal, isNotTimestamp, isLong);

    /* Float */
    int colRawDataFloatLocalCount = Mapper.getColumnCountLocal(cProfiles, colIdSTypeMap, isNotTimestamp, isFloat);
    List<List<Float>> rawDataFloatLocal = new ArrayList<>(colRawDataFloatLocalCount);
    fillArrayList(rawDataFloatLocal, colRawDataFloatLocalCount);
    List<Integer> rawDataFloatMappingLocal = new ArrayList<>(colRawDataFloatLocalCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataFloatMappingLocal, isNotTimestamp, isFloat);

    /* Double */
    int colRawDataDoubleLocalCount = Mapper.getColumnCountLocal(cProfiles, colIdSTypeMap, isNotTimestamp, isDouble);
    List<List<Double>> rawDataDoubleLocal = new ArrayList<>(colRawDataDoubleLocalCount);
    fillArrayList(rawDataDoubleLocal, colRawDataDoubleLocalCount);
    List<Integer> rawDataDoubleMappingLocal = new ArrayList<>(colRawDataDoubleLocalCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataDoubleMappingLocal, isNotTimestamp, isDouble);

    /* String */
    int colRawDataStringLocalCount = Mapper.getColumnCountLocal(cProfiles, colIdSTypeMap, isNotTimestamp, isString);
    List<List<String>> rawDataStringLocal = new ArrayList<>(colRawDataStringLocalCount);
    fillArrayList(rawDataStringLocal, colRawDataStringLocalCount);
    List<Integer> rawDataStringMappingLocal = new ArrayList<>(colRawDataStringLocalCount);
    fillAllExceptTimestampMapping(cProfiles, rawDataStringMappingLocal, isNotTimestamp, isString);

    cProfiles.forEach(cProfile -> {
      if (SType.RAW.equals(colIdSTypeMap.get(cProfile.getColId()))) {
        if (CType.INT == Mapper.isCType(cProfile)) {
          int index = rawDataIntMappingLocal.indexOf(cProfile.getColId());
          rawDataIntLocal.get(index).addAll(rawDataInt.get(index));
        } else if (CType.LONG == Mapper.isCType(cProfile)) {
          int index = rawDataLongMappingLocal.indexOf(cProfile.getColId());
          rawDataLongLocal.get(index).addAll(rawDataLong.get(index));
        } else if (CType.FLOAT == Mapper.isCType(cProfile)) {
          int index = rawDataFloatMappingLocal.indexOf(cProfile.getColId());
          rawDataFloatLocal.get(index).addAll(rawDataFloat.get(index));
        } else if (CType.DOUBLE == Mapper.isCType(cProfile)) {
          int index = rawDataDoubleMappingLocal.indexOf(cProfile.getColId());
          rawDataDoubleLocal.get(index).addAll(rawDataDouble.get(index));
        } else if (CType.STRING == Mapper.isCType(cProfile)) {
          int index = rawDataStringMappingLocal.indexOf(cProfile.getColId());
          rawDataStringLocal.get(index).addAll(rawDataString.get(index));
        }
      }
    });

    this.storeData(tableId, compression, blockId,
        rawDataTimeStampMapping, getArrayLong(rawDataTimestamp),
        colRawDataIntLocalCount, rawDataIntMappingLocal, getArrayInt(rawDataIntLocal),
        colRawDataLongLocalCount, rawDataLongMappingLocal, getArrayLong(rawDataLongLocal),
        colRawDataFloatLocalCount, rawDataFloatMappingLocal, getArrayFloat(rawDataFloatLocal),
        colRawDataDoubleLocalCount, rawDataDoubleMappingLocal, getArrayDouble(rawDataDoubleLocal),
        colRawDataStringLocalCount, rawDataStringMappingLocal, getArrayString(rawDataStringLocal),
        0, rawDataEnumMapping, getArrayByte(rawDataEnum), rawDataEnumEColumn);

    colIdSTypeMap.entrySet().stream()
        .filter(f -> !SType.HISTOGRAM.equals(f.getValue()))
        .forEach(obj -> mapOfHistograms.remove(obj.getKey()));

    this.storeHistograms(tableId, compression, blockId, mapOfHistograms);
  }

  private Map<Integer, Integer> mappingIndexToColId(List<Integer> rawMapping) {
    return IntStream.range(0, rawMapping.size())
            .boxed()
            .collect(Collectors.toMap(rawMapping::get, i -> i));
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
    fillMapping(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

    /* Long */
    int colRawDataLongCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isLong);
    long[][] rawDataLong = new long[colRawDataLongCount][rowCount];
    List<Integer> rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
    fillMapping(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

    /* Float */
    int colRawDataFloatCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isFloat);
    float[][] rawDataFloat = new float[colRawDataFloatCount][rowCount];
    List<Integer> rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
    fillMapping(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isDouble);
    double[][] rawDataDouble = new double[colRawDataDoubleCount][rowCount];
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMapping(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isNotTimestamp, isRaw, isString);
    String[][] rawDataString = new String[colRawDataStringCount][rowCount];
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMapping(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

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
          fillMapping(cProfiles, rawDataIntMapping, isNotTimestamp, isRaw, isInt);

          /* Long */
          rawDataLong = new long[colRawDataLongCount][rowCount];
          rawDataLongMapping = new ArrayList<>(colRawDataLongCount);
          fillMapping(cProfiles, rawDataLongMapping, isNotTimestamp, isRaw, isLong);

          /* Float */
          rawDataFloat = new float[colRawDataFloatCount][rowCount];
          rawDataFloatMapping = new ArrayList<>(colRawDataFloatCount);
          fillMapping(cProfiles, rawDataFloatMapping, isNotTimestamp, isRaw, isFloat);

          /* Double */
          rawDataDouble = new double[colRawDataDoubleCount][rowCount];
          rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
          fillMapping(cProfiles, rawDataDoubleMapping, isNotTimestamp, isRaw, isDouble);

          /* String */
          rawDataString = new String[colRawDataStringCount][rowCount];
          rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
          fillMapping(cProfiles, rawDataStringMapping, isNotTimestamp, isRaw, isString);

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
    fillMapping(cProfiles, rawDataLongMapping, isRaw, isLong);

    /* Double */
    int colRawDataDoubleCount = Mapper.getColumnCount(cProfiles, isRaw, isDouble);
    List<List<Double>> rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
    fillArrayList(rawDataDouble, colRawDataDoubleCount);
    List<Integer> rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
    fillMapping(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

    /* String */
    int colRawDataStringCount = Mapper.getColumnCount(cProfiles, isRaw, isString);
    List<List<String>> rawDataString = new ArrayList<>(colRawDataStringCount);
    fillArrayList(rawDataString, colRawDataStringCount);
    List<Integer> rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
    fillMapping(cProfiles, rawDataStringMapping, isRaw, isString);

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
          fillMapping(cProfiles, rawDataLongMapping, isRaw, isLong);

          /* Double */
          rawDataDouble = new ArrayList<>(colRawDataDoubleCount);
          fillArrayList(rawDataDouble, colRawDataDoubleCount);
          rawDataDoubleMapping = new ArrayList<>(colRawDataDoubleCount);
          fillMapping(cProfiles, rawDataDoubleMapping, isRaw, isDouble);

          /* String */
          rawDataString = new ArrayList<>(colRawDataStringCount);
          fillArrayList(rawDataString, colRawDataStringCount);
          rawDataStringMapping = new ArrayList<>(colRawDataStringCount);
          fillMapping(cProfiles, rawDataStringMapping, isRaw, isString);
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

  private void fillAllEnumMapping(List<CProfile> cProfiles, List<Integer> mapping,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn) {

    final AtomicInteger iRawDataEnumMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isNotTimestamp)
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
