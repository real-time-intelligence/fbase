package org.fbase.service.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.mapping.Mapper;
import org.fbase.storage.Converter;
import org.fbase.util.CachedLastLinkedHashMap;

@Getter
@EqualsAndHashCode(callSuper=true)
public class RStore extends CommonServiceApi {
  private final Converter converter;

  private final int initialCapacityInt;
  private final int initialCapacityLong;
  private final int initialCapacityFloat;
  private final int initialCapacityDouble;
  private final int initialCapacityString;

  private final List<List<Integer>> rawDataInt;
  private final List<List<Long>> rawDataLong;
  private final List<List<Float>> rawDataFloat;
  private final List<List<Double>> rawDataDouble;
  private final List<List<String>> rawDataString;

  private final CachedLastLinkedHashMap<Integer, Integer> mappingInt;
  private final CachedLastLinkedHashMap<Integer, Integer> mappingLong;
  private final CachedLastLinkedHashMap<Integer, Integer> mappingFloat;
  private final CachedLastLinkedHashMap<Integer, Integer> mappingDouble;
  private final CachedLastLinkedHashMap<Integer, Integer> mappingString;

  public RStore(Converter converter, List<CProfile> cProfiles, Map<Integer, SType> colIdSTypeMap) {
    this.converter = converter;

    /* Int */
    initialCapacityInt = Mapper.getColumnCount(cProfiles, colIdSTypeMap, isNotTimestamp, isInt);
    rawDataInt = new ArrayList<>(initialCapacityInt);
    mappingInt = new CachedLastLinkedHashMap<>();
    fillArrayList(rawDataInt, initialCapacityInt);
    fillMappingRaw(cProfiles, mappingInt, colIdSTypeMap, isNotTimestamp, isRaw, isInt);

    /* Long */
    initialCapacityLong = Mapper.getColumnCount(cProfiles, colIdSTypeMap, isNotTimestamp, isLong);
    rawDataLong = new ArrayList<>(initialCapacityLong);
    mappingLong = new CachedLastLinkedHashMap<>();
    fillArrayList(rawDataLong, initialCapacityLong);
    fillMappingRaw(cProfiles, mappingLong, colIdSTypeMap, isNotTimestamp, isRaw, isLong);

    /* Float */
    initialCapacityFloat = Mapper.getColumnCount(cProfiles, colIdSTypeMap, isNotTimestamp, isFloat);
    rawDataFloat = new ArrayList<>(initialCapacityFloat);
    mappingFloat = new CachedLastLinkedHashMap<>();
    fillArrayList(rawDataFloat, initialCapacityFloat);
    fillMappingRaw(cProfiles, mappingFloat, colIdSTypeMap, isNotTimestamp, isRaw, isFloat);

    /* Double */
    initialCapacityDouble = Mapper.getColumnCount(cProfiles, colIdSTypeMap, isNotTimestamp, isDouble);
    rawDataDouble = new ArrayList<>(initialCapacityDouble);
    mappingDouble = new CachedLastLinkedHashMap<>();
    fillArrayList(rawDataDouble, initialCapacityDouble);
    fillMappingRaw(cProfiles, mappingDouble, colIdSTypeMap, isNotTimestamp, isRaw, isDouble);

    /* String */
    initialCapacityString = Mapper.getColumnCount(cProfiles, colIdSTypeMap, isNotTimestamp, isString);
    rawDataString = new ArrayList<>(initialCapacityString);
    mappingString = new CachedLastLinkedHashMap<>();
    fillArrayList(rawDataString, initialCapacityString);
    fillMappingRaw(cProfiles, mappingString, colIdSTypeMap, isNotTimestamp, isRaw, isString);
  }

  public void add(CType cType, CProfile cProfile, int iR, int key) {
    int colId = cProfile.getColId();

    if (CType.INT == cType) {
      this.rawDataInt.get(mappingInt.get(colId)).add(iR, key);
    } else if (CType.LONG == cType) {
      this.rawDataLong.get(mappingLong.get(colId)).add(iR, (long) key);
    } else if (CType.FLOAT == cType) {
      this.rawDataFloat.get(mappingFloat.get(colId)).add(iR, (float) converter.convertIntToDouble(key, cProfile));
    } else if (CType.DOUBLE == cType) {
      this.rawDataDouble.get(mappingDouble.get(colId)).add(iR, converter.convertIntToDouble(key, cProfile));
    } else if (CType.STRING == cType) {
      this.rawDataString.get(mappingString.get(colId)).add(iR, converter.convertIntToRaw(key, cProfile));
    }
  }

  public void add(CProfile cProfile, int iR, Object currObject) {
    CType cType = Mapper.isCType(cProfile);
    int colId = cProfile.getColId();
    if (CType.INT == cType) {
      rawDataInt.get(mappingInt.get(colId)).add(iR, Mapper.convertRawToInt(currObject, cProfile));
    } else if (CType.LONG == cType) {
      rawDataLong.get(mappingLong.get(colId)).add(iR, Mapper.convertRawToLong(currObject, cProfile));
    } else if (CType.FLOAT == cType) {
      rawDataFloat.get(mappingFloat.get(colId)).add(iR, Mapper.convertRawToFloat(currObject, cProfile));
    } else if (CType.DOUBLE == cType) {
      rawDataDouble.get(mappingDouble.get(colId)).add(iR, Mapper.convertRawToDouble(currObject, cProfile));
    } else if (CType.STRING == cType) {
      rawDataString.get(mappingString.get(colId)).add(iR, Mapper.convertRawToString(currObject, cProfile));
    }
  }
}
