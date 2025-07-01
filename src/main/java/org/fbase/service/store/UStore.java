package org.fbase.service.store;

import static org.fbase.service.mapping.Mapper.INT_NULL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.mapping.Mapper;
import org.fbase.storage.Converter;
import org.fbase.storage.helper.EnumHelper;
import org.fbase.util.CachedLastLinkedHashMap;

@Getter
@EqualsAndHashCode(callSuper = true)
public class UStore extends CommonServiceApi {

  private final Converter converter;
  private final Map<Integer, List<Object>> rawDataMap;        // For RAW storage
  private final Map<Integer, List<Byte>> enumDataMap;         // For ENUM storage
  private final Map<Integer, HEntry> histogramDataMap;        // For HISTOGRAM storage
  private final Map<Integer, SType> storageTypeMap;           // Tracks storage type per column
  private final Map<Integer, CachedLastLinkedHashMap<Integer, Byte>> enumDictionaries; // Enum value mappings

  public UStore(Converter converter) {
    this.converter = converter;
    this.rawDataMap = new HashMap<>();
    this.enumDataMap = new HashMap<>();
    this.histogramDataMap = new HashMap<>();
    this.storageTypeMap = new HashMap<>();
    this.enumDictionaries = new CachedLastLinkedHashMap<>();
  }

  public void addColumn(CProfile cProfile, SType sType) {
    int colId = cProfile.getColId();
    if (storageTypeMap.containsKey(colId)) {
      throw new IllegalArgumentException("Column already exists: " + colId);
    }

    storageTypeMap.put(colId, sType);
    if (sType == SType.RAW) {
      rawDataMap.put(colId, new ArrayList<>());
    } else if (sType == SType.ENUM) {
      enumDataMap.put(colId, new ArrayList<>());
      enumDictionaries.put(colId, new CachedLastLinkedHashMap<>());
    } else if (sType == SType.HISTOGRAM) {
      histogramDataMap.put(colId, new HEntry(new ArrayList<>(), new ArrayList<>()));
    }
  }

  public void removeColumn(CProfile cProfile) {
    int colId = cProfile.getColId();
    SType sType = storageTypeMap.remove(colId);

    if (sType == SType.RAW) {
      rawDataMap.remove(colId);
    } else if (sType == SType.ENUM) {
      enumDictionaries.remove(colId);
      enumDataMap.remove(colId);
    } else if (sType == SType.HISTOGRAM) {
      histogramDataMap.remove(colId);
    }
  }

  public void add(CProfile cProfile, int iR, Object currObject) {
    int colId = cProfile.getColId();
    SType sType = storageTypeMap.get(colId);
    CType cType = Mapper.isCType(cProfile);

    if (sType == SType.RAW) {
      Object value = convertRawValue(currObject, cProfile, cType);
      insertRawValue(colId, iR, value);
    } else if (sType == SType.ENUM) {
      int intValue = converter.convertRawToInt(currObject, cProfile);
      insertEnumValue(colId, iR, intValue, cProfile);
    } else if (sType == SType.HISTOGRAM) {
      int intValue = converter.convertRawToInt(currObject, cProfile);
      insertHistogramValue(colId, iR, intValue);
    }
  }

  // Helper methods
  private void insertRawValue(int colId, int iR, Object value) {
    List<Object> columnData = rawDataMap.get(colId);
    if (columnData == null) {
      throw new IllegalArgumentException("RAW column not initialized: " + colId);
    }
    columnData.add(iR, value);
  }

  private void insertEnumValue(int colId, int iR, int value, CProfile cProfile) {
    List<Byte> columnData = enumDataMap.get(colId);
    CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);

    if (columnData == null || dictionary == null) {
      throw new IllegalArgumentException("ENUM column not initialized: " + colId);
    }

    try {
      byte byteValue = EnumHelper.getByteValue(dictionary, value);
      columnData.add(iR, byteValue);
    } catch (EnumByteExceedException e) {
      // Fallback to RAW: convert existing ENUM data to RAW
      Map<Byte, Integer> reverseMap = new HashMap<>();
      for (Map.Entry<Integer, Byte> entry : dictionary.entrySet()) {
        reverseMap.put(entry.getValue(), entry.getKey());
      }

      List<Object> rawData = new ArrayList<>();
      for (byte b : columnData) {
        Integer intValue = reverseMap.get(b);
        if (intValue == null) {
          throw new IllegalStateException("Enum value not found for byte: " + b);
        }
        rawData.add(convertIntToRawValue(intValue, cProfile));
      }
      rawData.add(iR, convertIntToRawValue(value, cProfile));

      // Update storage structures
      enumDataMap.remove(colId);
      enumDictionaries.remove(colId);
      storageTypeMap.put(colId, SType.RAW);
      rawDataMap.put(colId, rawData);
    }
  }

  private Object convertIntToRawValue(int value, CProfile cProfile) {
    CType cType = Mapper.isCType(cProfile);
    if (cType == CType.STRING) {
      return converter.convertIntToRaw(value, cProfile);
    } else {
      return convertIntToObject(value, cType);
    }
  }

  private Object convertRawValue(Object currObject,
                                 CProfile cProfile,
                                 CType cType) {
    switch (cType) {
      case INT: return Mapper.convertRawToInt(currObject, cProfile);
      case LONG: return Mapper.convertRawToLong(currObject, cProfile);
      case FLOAT: return Mapper.convertRawToFloat(currObject, cProfile);
      case DOUBLE: return Mapper.convertRawToDouble(currObject, cProfile);
      case STRING: return Mapper.convertRawToString(currObject, cProfile);
      default: throw new IllegalArgumentException("Unsupported type: " + cType);
    }
  }

  private void insertHistogramValue(int colId, int iR, int value) {
    HEntry entry = histogramDataMap.get(colId);
    if (entry == null) {
      throw new IllegalArgumentException("HISTOGRAM column not initialized: " + colId);
    }

    List<Integer> indices = entry.getIndex();
    List<Integer> values = entry.getValue();
    int prevValue = values.isEmpty() ? INT_NULL : values.get(values.size() - 1);

    if (prevValue != value || indices.isEmpty()) {
      indices.add(iR);
      values.add(value);
    }
  }

  public void analyzeAndConvertColumns(int totalRowCount, Map<Integer, CProfile> cProfileMap) {
    for (Map.Entry<Integer, SType> entry : storageTypeMap.entrySet()) {
      int colId = entry.getKey();
      SType currentType = entry.getValue();
      CProfile cProfile = cProfileMap.get(colId);
      if (cProfile == null) continue;

      // Skip non-numeric columns
      CType cType = Mapper.isCType(cProfile);

      List<Integer> intValues = getIntegerListForColumn(colId, currentType, totalRowCount, cProfile);
      if (intValues == null || intValues.size() != totalRowCount) continue;

      // Calculate distinct values and runs
      Set<Integer> distinct = new HashSet<>(intValues);
      int ndv = distinct.size();
      int runs = 1;
      for (int i = 1; i < intValues.size(); i++) {
        if (!intValues.get(i).equals(intValues.get(i - 1))) {
          runs++;
        }
      }

      // Calculate clustering index
      double clusteringIndex;
      if (totalRowCount == ndv) {
        clusteringIndex = 0.0;
      } else {
        clusteringIndex = (totalRowCount - runs) / (double) (totalRowCount - ndv);
      }

      // Determine new storage type
      SType newType;
      if (ndv > 0.5 * totalRowCount) {
        newType = SType.RAW;
      } else {
        if (clusteringIndex >= 0.8) {
          newType = SType.HISTOGRAM;
        } else {
          newType = SType.ENUM;
        }
      }

      if (newType == currentType) continue;

      // Remove column and convert to new storage type
      removeColumn(cProfile);
      convertToNewStorage(colId, cProfile, newType, intValues, cType);
    }
  }

  List<Integer> getIntegerListForColumn(int colId, SType currentType, int totalRowCount, CProfile cProfile) {
    if (currentType == SType.RAW) {
      List<Object> rawData = rawDataMap.get(colId);
      if (rawData == null) return null;
      List<Integer> intValues = new ArrayList<>();
      for (Object obj : rawData) {
        intValues.add(converter.convertRawToInt(obj, cProfile));
      }
      return intValues;
    } else if (currentType == SType.ENUM) {
      List<Byte> enumData = enumDataMap.get(colId);
      CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);
      Map<Byte, Integer> reverseMap = new HashMap<>();
      for (Map.Entry<Integer, Byte> e : dictionary.entrySet()) {
        reverseMap.put(e.getValue(), e.getKey());
      }
      List<Integer> intValues = new ArrayList<>();
      for (byte b : enumData) {
        Integer value = reverseMap.get(b);
        if (value == null) return null;
        intValues.add(value);
      }
      return intValues;
    } else if (currentType == SType.HISTOGRAM) {
      HEntry entry = histogramDataMap.get(colId);
      List<Integer> indices = entry.getIndex();
      List<Integer> values = entry.getValue();
      List<Integer> intValues = new ArrayList<>(totalRowCount);
      int currentPtr = 0;
      int currentVal = INT_NULL;
      for (int r = 0; r < totalRowCount; r++) {
        if (currentPtr < indices.size() && r == indices.get(currentPtr)) {
          currentVal = values.get(currentPtr);
          currentPtr++;
        }
        intValues.add(currentVal);
      }
      return intValues;
    }
    return null;
  }

  private void convertToNewStorage(int colId, CProfile cProfile, SType newType, List<Integer> intValues, CType cType) {
    addColumn(cProfile, newType);
    if (newType == SType.RAW) {
      List<Object> rawData = new ArrayList<>();
      for (int value : intValues) {
        if (cType == CType.STRING) {
          rawData.add(converter.convertIntToRaw(value, cProfile));
        } else {
          rawData.add(convertIntToObject(value, cType));
        }
      }
      rawDataMap.put(colId, rawData);
    } else if (newType == SType.ENUM) {
      List<Byte> enumData = enumDataMap.get(colId);
      CachedLastLinkedHashMap<Integer, Byte> dictionary = enumDictionaries.get(colId);
      dictionary.clear();
      for (int value : intValues) {
        try {
          enumData.add(EnumHelper.getByteValue(dictionary, value));
        } catch (EnumByteExceedException e) {
          // Fallback to RAW if enum conversion fails
          removeColumn(cProfile);
          addColumn(cProfile, SType.RAW);
          List<Object> rawData = new ArrayList<>();
          for (int v : intValues) {
            rawData.add(convertIntToObject(v, cType));
          }
          rawDataMap.put(colId, rawData);
          return;
        }
      }
    } else if (newType == SType.HISTOGRAM) {
      HEntry entry = histogramDataMap.get(colId);
      List<Integer> indices = entry.getIndex();
      List<Integer> values = entry.getValue();
      int prevValue = INT_NULL;
      for (int r = 0; r < intValues.size(); r++) {
        int currValue = intValues.get(r);
        if (r == 0 || currValue != prevValue) {
          indices.add(r);
          values.add(currValue);
          prevValue = currValue;
        }
      }
    }
  }

  public SType analyzeColumn(int colId, int totalRowCount, CProfile cProfile) {
    SType currentType = storageTypeMap.get(colId);
    List<Integer> intValues = getIntegerListForColumn(colId, currentType, totalRowCount, cProfile);

    if (intValues == null || intValues.size() != totalRowCount) {
      return currentType;
    }

    // Calculate distinct values and runs
    Set<Integer> distinct = new HashSet<>(intValues);
    int ndv = distinct.size();
    int runs = 1;
    for (int i = 1; i < intValues.size(); i++) {
      if (!intValues.get(i).equals(intValues.get(i - 1))) {
        runs++;
      }
    }

    // Calculate clustering index
    double clusteringIndex;
    if (totalRowCount == ndv) {
      clusteringIndex = 0.0;
    } else {
      clusteringIndex = (totalRowCount - runs) / (double) (totalRowCount - ndv);
    }

    // Determine new storage type
    SType newType;
    if (ndv > 0.5 * totalRowCount) {
      newType = SType.RAW;
    } else {
      if (clusteringIndex >= 0.8) {
        newType = SType.HISTOGRAM;
      } else {
        newType = SType.ENUM;
      }
    }

    return newType;
  }

  private Object convertIntToObject(int value, CType cType) {
    return switch (cType) {
      case INT -> value;
      case LONG -> (long) value;
      case FLOAT -> (float) value;
      case DOUBLE -> (double) value;
      default -> value;
    };
  }

}