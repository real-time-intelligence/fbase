package org.fbase.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.Converter;
import org.fbase.core.Mapper;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.StoreService;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class StoreServiceImpl extends CommonServiceApi implements StoreService {

  private final MetaModel metaModel;

  private final Converter converter;

  private final EnumDAO enumDAO;

  private final RawDAO rawDAO;

  private final HistogramDAO histogramDAO;

  private final MetadataDAO metadataDAO;

  Predicate<CProfile> isNotTimestamp = Predicate.not(f -> f.getCsType().isTimeStamp());
  Predicate<CProfile> isRaw = Predicate.not(f -> f.getCsType().getSType() != SType.RAW);
  Predicate<CProfile> isEnum = Predicate.not(f -> f.getCsType().getSType() != SType.ENUM);
  Predicate<CProfile> isHistogram = Predicate.not(f -> f.getCsType().getSType() != SType.HISTOGRAM);
  Predicate<CProfile> isInt = Predicate.not(f -> Mapper.isCType(f) != CType.INT);
  Predicate<CProfile> isLong = Predicate.not(f -> Mapper.isCType(f) != CType.LONG);
  Predicate<CProfile> isFloat = Predicate.not(f -> Mapper.isCType(f) != CType.FLOAT);
  Predicate<CProfile> isDouble = Predicate.not(f -> Mapper.isCType(f) != CType.DOUBLE);
  Predicate<CProfile> isString = Predicate.not(f -> Mapper.isCType(f) != CType.STRING);

  public StoreServiceImpl(MetaModel metaModel, Converter converter,
      RawDAO rawDAO, EnumDAO enumDAO, HistogramDAO histogramDAO, MetadataDAO metadataDAO) {
    this.metaModel = metaModel;

    this.converter = converter;

    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
    this.histogramDAO = histogramDAO;
    this.metadataDAO = metadataDAO;
  }

  @Override
  public void putDataDirect(TProfile tProfile, List<List<Object>> data) {
    byte tableId = getTableId(tProfile, metaModel);
    int rowCount = data.get(0).size();

    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);
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
    List<int[]> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(tableId, cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

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
            int valueByte = EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt);
            rawDataEnum[rawDataEnumMapping.indexOf(iC)][iR] = (byte) valueByte;
          } catch (EnumByteExceedException e) {
            log.error(e.getMessage());
          }
        }

      });
    });

    // Fill histogram data
    Map<Integer, Map<Integer, Integer>> mapOfHistograms = new HashMap<>(colCount);
    cProfiles.stream().filter(isHistogram).forEach(e -> mapOfHistograms.put(e.getColId(), new LinkedHashMap<>()));

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

    int[] histograms = getHistograms(colCount, mapOfHistograms);

    long key = rawDataTimestamp[0][0];

    this.storeData(tableId, cProfiles, key,
        rawDataTimeStampMapping, rawDataTimestamp,
        colRawDataIntCount, rawDataIntMapping, rawDataInt,
        colRawDataLongCount, rawDataLongMapping, rawDataLong,
        colRawDataFloatCount, rawDataFloatMapping, rawDataFloat,
        colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
        colRawDataStringCount, rawDataStringMapping, rawDataString,
        colRawDataEnumCount, rawDataEnumMapping, rawDataEnum, rawDataEnumEColumn,
        histograms);
  }

  @Override
  public long putDataJdbc(TProfile tProfile, ResultSet resultSet) {
    byte tableId = getTableId(tProfile, metaModel);

    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);
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
    List<int[]> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(tableId, cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

    // Fill histogram data
    Map<Integer, Map<Integer, Integer>> mapOfHistograms = new HashMap<>();
    cProfiles.stream().filter(isHistogram).forEach(e -> mapOfHistograms.put(e.getColId(), new LinkedHashMap<>()));

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
                int valueByte = EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt);
                rawDataEnum.get(rawDataEnumMapping.indexOf(iC)).add(iR, (byte) valueByte);
              } catch (EnumByteExceedException e) {
                log.error(e.getMessage());
              }
            }

            // Fill histogram data
            if (csType.getSType() == SType.HISTOGRAM) {
              int currInt = this.converter.convertRawToInt(currObject, cProfile);

              if (iR != 0) {
                Integer prevObject = mapOfHistograms.get(cProfile.getColId()).get(iR - 1);
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

      int[] histograms = getHistograms(colCount, mapOfHistograms);

      long key = rawDataTimestamp.get(0).get(0);

      this.storeData(tableId, cProfiles, key,
          rawDataTimeStampMapping, getArrayLong(rawDataTimestamp),
          colRawDataIntCount, rawDataIntMapping, getArrayInt(rawDataInt),
          colRawDataLongCount, rawDataLongMapping, getArrayLong(rawDataLong),
          colRawDataFloatCount, rawDataFloatMapping, getArrayFloat(rawDataFloat),
          colRawDataDoubleCount, rawDataDoubleMapping, getArrayDouble(rawDataDouble),
          colRawDataStringCount, rawDataStringMapping, getArrayString(rawDataString),
          colRawDataEnumCount, rawDataEnumMapping, getArrayByte(rawDataEnum), rawDataEnumEColumn,
          histograms);

      return rawDataTimestamp.get(0).get(rawDataTimestamp.get(0).size() - 1);

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void putDataBatch(TProfile tProfile, ResultSet resultSet, Integer fBaseBatchSize) {
    byte tableId = getTableId(tProfile, metaModel);

    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);
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
    List<int[]> rawDataEnumEColumn = new ArrayList<>(colRawDataEnumCount);
    fillEnumMapping(tableId, cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

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
          int[] histograms = getHistogramsCachedLast(colCount, mapOfHistograms);

          long key = rawDataTimestamp[0][0];

          this.storeData(tableId, cProfiles, key,
              rawDataTimeStampMapping, rawDataTimestamp,
              colRawDataIntCount, rawDataIntMapping, rawDataInt,
              colRawDataLongCount, rawDataLongMapping, rawDataLong,
              colRawDataFloatCount, rawDataFloatMapping, rawDataFloat,
              colRawDataDoubleCount, rawDataDoubleMapping, rawDataDouble,
              colRawDataStringCount, rawDataStringMapping, rawDataString,
              colRawDataEnumCount, rawDataEnumMapping, rawDataEnum, rawDataEnumEColumn,
              histograms);

          log.info("Flush for iRow: " + iR);

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
          fillEnumMapping(tableId, cProfiles, rawDataEnumMapping, rawDataEnumEColumn);

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
                int valueByte = EnumHelper.getByteValue(rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(iC)), valueInt);
                rawDataEnum[rawDataEnumMapping.indexOf(iC)][iR] = (byte) valueByte;
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
        int[] histograms = getHistogramsCachedLast(colCount, mapOfHistograms);

        long key = rawDataTimestamp[0][0];

        int row = iRow.get();

        log.info("Final flush for iRow: " + iRow.get());

        this.storeData(tableId, cProfiles, key,
            rawDataTimeStampMapping, copyOfLong(rawDataTimestamp, row),
            colRawDataIntCount, rawDataIntMapping, copyOfInt(rawDataInt, row),
            colRawDataLongCount, rawDataLongMapping, copyOfLong(rawDataLong, row),
            colRawDataFloatCount, rawDataFloatMapping, copyOfFloat(rawDataFloat, row),
            colRawDataDoubleCount, rawDataDoubleMapping, copyOfDouble(rawDataDouble, row),
            colRawDataStringCount, rawDataStringMapping, copyOfString(rawDataString, row),
            colRawDataEnumCount, rawDataEnumMapping, copyOfByte(rawDataEnum, row), rawDataEnumEColumn,
            histograms);
      }

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

  }

  private int getResultSetSize(ResultSet resultSet) {
    int size;
    try {
      resultSet.last();
      size = resultSet.getRow();
      resultSet.beforeFirst();
    } catch(Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

    return size;
  }

  public void fillEnumMapping(byte tableId, List<CProfile> cProfiles, List<Integer> mapping, List<int[]> rawDataEnumEColumn) {
    final AtomicInteger iRawDataEnumMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isEnum)
        .forEach(e -> {
          int var = iRawDataEnumMapping.getAndAdd(1);
          mapping.add(var, e.getColId());
          rawDataEnumEColumn.add(var, this.enumDAO.putEColumn(tableId, e.getColId()).getValues());
        });
  }

  private int[] getHistograms(int colCount, Map<Integer, Map<Integer, Integer>> mapOfHistograms) {
    int[] histograms = new int[colCount];

    mapOfHistograms.forEach((k, v) -> {
      if (!v.isEmpty()) {
        int indexValue = this.histogramDAO.put(getArrayFromMap(v));
        histograms[k] = indexValue;
      } else {
        histograms[k] = -1;
      }
    });

    return histograms;
  }

  private int[] getHistogramsCachedLast(int colCount, Map<Integer, CachedLastLinkedHashMap<Integer, Integer>> mapOfHistograms) {
    int[] histograms = new int[colCount];

    mapOfHistograms.forEach((k, v) -> {
      if (!v.isEmpty()) {
        int indexValue = this.histogramDAO.put(getArrayFromMap(v));
        histograms[k] = indexValue;
      } else {
        histograms[k] = -1;
      }
    });

    return histograms;
  }

  private void storeData(byte tableId, List<CProfile> cProfiles, long key,
      List<Integer> rawDataTimeStampMapping, long[][] rawDataTimestamp,
      int colRawDataIntCount, List<Integer> rawDataIntMapping, int[][] rawDataInt,
      int colRawDataLongCount, List<Integer> rawDataLongMapping, long[][] rawDataLong,
      int colRawDataFloatCount, List<Integer> rawDataFloatMapping, float[][] rawDataFloat,
      int colRawDataDoubleCount, List<Integer> rawDataDoubleMapping, double[][] rawDataDouble,
      int colRawDataStringCount, List<Integer> rawDataStringMapping, String[][] rawDataString,
      int colRawDataEnumCount, List<Integer> rawDataEnumMapping, byte[][] rawDataEnum, List<int[]> rawDataEnumEColumn,
      int[] histograms) {

    /* Store data in RMapping entity */
    this.rawDAO.putKey(tableId, key);

    /* Store timestamp raw data entity */
    this.rawDAO.putLong(tableId, key, rawDataTimeStampMapping.stream().mapToInt(i -> i).toArray(), rawDataTimestamp);

    /* Store raw data entity */
    if (colRawDataIntCount > 0)
      this.rawDAO.putInt(tableId, key, rawDataIntMapping.stream().mapToInt(i -> i).toArray(), rawDataInt);
    if (colRawDataLongCount > 0)
      this.rawDAO.putLong(tableId, key, rawDataLongMapping.stream().mapToInt(i -> i).toArray(), rawDataLong);
    if (colRawDataFloatCount > 0)
      this.rawDAO.putFloat(tableId, key, rawDataFloatMapping.stream().mapToInt(i -> i).toArray(), rawDataFloat);
    if (colRawDataDoubleCount > 0)
      this.rawDAO.putDouble(tableId, key, rawDataDoubleMapping.stream().mapToInt(i -> i).toArray(), rawDataDouble);
    if (colRawDataStringCount > 0)
      this.rawDAO.putString(tableId, key, rawDataStringMapping.stream().mapToInt(i -> i).toArray(), rawDataString);

    /* Store enum raw data entity */
    if (colRawDataEnumCount > 0) {
      this.rawDAO.putEnum(tableId, key, rawDataEnumMapping.stream().mapToInt(i -> i).toArray(), rawDataEnum);

      cProfiles.stream()
          .filter(isEnum)
          .forEach(e -> this.enumDAO.updateEColumnValues(tableId, e.getColId(),
              rawDataEnumEColumn.get(rawDataEnumMapping.indexOf(e.getColId()))));
    }

    /* Store metadata entity */
    this.metadataDAO.put(tableId, key, getByteFromList(new ArrayList<>()), getByteFromList(new ArrayList<>()), histograms);
  }

  static class CachedLastLinkedHashMap<K,V> extends LinkedHashMap<K, V> {
    private V last = null;

    @Override
    public V put(K key, V value) {
      last = value;
      return super.put(key, value);
    }

    public V getLast() {
      return last;
    }
  }

}
