package org.fbase.service;

import static org.fbase.service.mapping.Mapper.DOUBLE_NULL;
import static org.fbase.service.mapping.Mapper.FLOAT_NULL;
import static org.fbase.service.mapping.Mapper.INT_NULL;
import static org.fbase.service.mapping.Mapper.LONG_NULL;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.service.mapping.Mapper;
import org.fbase.service.store.HEntry;
import org.fbase.storage.RawDAO;
import org.fbase.util.CachedLastLinkedHashMap;

public abstract class CommonServiceApi {

  public Predicate<CProfile> isNotTimestamp = Predicate.not(f -> f.getCsType().isTimeStamp());
  public Predicate<CProfile> isRaw = Predicate.not(f -> f.getCsType().getSType() != SType.RAW);
  public Predicate<CProfile> isEnum = Predicate.not(f -> f.getCsType().getSType() != SType.ENUM);
  public Predicate<CProfile> isHistogram = Predicate.not(f -> f.getCsType().getSType() != SType.HISTOGRAM);
  public Predicate<CProfile> isInt = Predicate.not(f -> Mapper.isCType(f) != CType.INT);
  public Predicate<CProfile> isLong = Predicate.not(f -> Mapper.isCType(f) != CType.LONG);
  public Predicate<CProfile> isFloat = Predicate.not(f -> Mapper.isCType(f) != CType.FLOAT);
  public Predicate<CProfile> isDouble = Predicate.not(f -> Mapper.isCType(f) != CType.DOUBLE);
  public Predicate<CProfile> isString = Predicate.not(f -> Mapper.isCType(f) != CType.STRING);

  protected int getHistogramValue(int iR, int[][] histogram, long[] timestamps) {
    int curValue = 0;

    for (int i = 0; i < histogram[0].length; i++) {
      int curIndex = histogram[0][i];
      int nextIndex;

      curValue = histogram[1][i];

      if (histogram[0].length != i+1) {
        nextIndex = histogram[0][i+1];

        if (iR >= curIndex & iR < nextIndex) {
          return curValue;
        }

      } else {
        nextIndex = timestamps.length - 1;

        if (iR >= curIndex & iR < nextIndex) {
          return curValue;
        }

        if (nextIndex == iR) {
          return curValue;
        }
      }
    }
    return curValue;
  }

  public int[][] getArrayFromMapHEntry(HEntry hEntry) {
    int[][] array = new int[2][hEntry.getIndex().size()];

    System.arraycopy(hEntry.getIndex()
        .stream().mapToInt(Integer::intValue).toArray(), 0, array[0], 0, hEntry.getIndex().size());
    System.arraycopy(hEntry.getValue().stream()
        .mapToInt(Integer::intValue).toArray(), 0, array[1], 0, hEntry.getValue().size());

    return array;
  }

  public byte getTableId(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getTableId();
  }

  public TType getTableType(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getTableType();
  }

  public IType getIndexType(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getIndexType();
  }

  public Boolean getTableCompression(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getCompression();
  }

  public List<CProfile> getCProfiles(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getCProfiles();
  }

  public CProfile getTimestampProfile(List<CProfile> cProfileList) {
   return cProfileList.stream()
          .filter(k -> k.getCsType().isTimeStamp())
          .findAny()
          .orElseThrow(() -> new RuntimeException("Not found timestamp column"));
  }

  protected <T, V> void setMapValue(Map<T, Map<V, Integer>> map, T vFirst, V vSecond, int sum) {
    if (map.get(vFirst) == null) {
      map.put(vFirst, new HashMap<>());
      map.get(vFirst).putIfAbsent(vSecond, sum);
    } else {
      if (map.get(vFirst).get(vSecond) == null) {
        map.get(vFirst).putIfAbsent(vSecond, sum);
      } else {
        map.get(vFirst).computeIfPresent(vSecond, (k, v) -> v + sum);
      }
    }
  }

  protected void setMapValueEnumBlock(Map<Integer, Map<Integer, Integer>> map, Integer vFirst, int vSecond, int sum) {
    if (map.get(vFirst) == null) {
      map.put(vFirst, new HashMap<>());
      map.get(vFirst).putIfAbsent(vSecond, sum);
    } else {
      if (map.get(vFirst).get(vSecond) == null) {
        map.get(vFirst).putIfAbsent(vSecond, sum);
      } else {
        map.get(vFirst).computeIfPresent(vSecond, (k, v) -> v + sum);
      }
    }
  }

  protected void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map, String vFirst, int vSecond, int sum) {
    if (map.get(vFirst) == null) {
      map.put(vFirst, new HashMap<>());
      map.get(vFirst).putIfAbsent(vSecond, sum);
    } else {
      if (map.get(vFirst).get(vSecond) == null) {
        map.get(vFirst).putIfAbsent(vSecond, sum);
      } else {
        map.get(vFirst).computeIfPresent(vSecond, (k, v) -> v + sum);
      }
    }
  }

  protected void setMapValueRawEnumBlock(Map<Integer, Map<String, Integer>> map, int vFirst, String vSecond, int sum) {
    if (map.get(vFirst) == null) {
      map.put(vFirst, new HashMap<>());
      map.get(vFirst).putIfAbsent(vSecond, sum);
    } else {
      if (map.get(vFirst).get(vSecond) == null) {
        map.get(vFirst).putIfAbsent(vSecond, sum);
      } else {
        map.get(vFirst).computeIfPresent(vSecond, (k, v) -> v + sum);
      }
    }
  }

  protected <T> void fillArrayList(List<List<T>> array, int colCount) {
    for (int i = 0; i < colCount; i++) {
      array.add(new ArrayList<>());
    }
  }

  protected int[][] getArrayInt(List<List<Integer>> rawDataInt) {
    return rawDataInt.stream()
        .map(l -> l.stream().mapToInt(Integer::intValue).toArray())
        .toArray(int[][]::new);
  }

  protected long[][] getArrayLong(List<List<Long>> rawDataLong) {
    return rawDataLong.stream()
        .map(l -> l.stream().mapToLong(Long::longValue).toArray())
        .toArray(long[][]::new);
  }

  protected double[][] getArrayDouble(List<List<Double>> rawDataDouble) {
    return rawDataDouble.stream()
        .map(l -> l.stream().mapToDouble(Double::doubleValue).toArray())
        .toArray(double[][]::new);
  }

  protected float[][] getArrayFloat(List<List<Float>> rawDataFloat) {
    float[][] array = new float[rawDataFloat.size()][];
    for (int i = 0; i < rawDataFloat.size(); i++) {
      List<Float> row = rawDataFloat.get(i);
      array[i] = row.stream().collect(
          ()-> FloatBuffer.allocate(row.size()),
          FloatBuffer::put,
          (left, right) -> {throw new UnsupportedOperationException("Only called in parallel stream");}).array();
    }
    return array;
  }

  protected String[][] getArrayString(List<List<String>> rawDataString) {
    String[][] array = new String[rawDataString.size()][];
    for (int i = 0; i < rawDataString.size(); i++) {
      List<String> row = rawDataString.get(i);
      array[i] = getStringFromList(row);
    }
    return array;
  }

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }
    return byteArray;
  }

  public String[] getStringFromList(List<String> list) {
    String[] stringArray = new String[list.size()];
    int index = 0;
    for (String b : list) {
      stringArray[index++] = b;
    }
    return stringArray;
  }

  public void fillTimestampMap(List<CProfile> cProfiles, CachedLastLinkedHashMap<Integer, Integer> mapping) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(f -> f.getCsType().isTimeStamp())
        .forEach(e -> mapping.put(e.getColId(), iRawDataLongMapping.getAndAdd(1)));
  }

  public void fillAllEnumMappingSType(List<CProfile> cProfiles, CachedLastLinkedHashMap<Integer, Integer> mapping,
      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn, Map<Integer, SType> colIdSTypeMap) {

    final AtomicInteger iRawDataEnumMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(f -> !f.getCsType().isTimeStamp())
        .filter(f -> SType.ENUM.equals(colIdSTypeMap.get(f.getColId())))
        .forEach(cProfile -> {
          int var = iRawDataEnumMapping.getAndAdd(1);
          mapping.put(cProfile.getColId(), var);
          rawDataEnumEColumn.add(var, new CachedLastLinkedHashMap<>());
        });
  }

  public void fillMappingRaw(List<CProfile> cProfiles,
      CachedLastLinkedHashMap<Integer, Integer> mapping, Map<Integer, SType> colIdSTypeMap,
      Predicate<CProfile> isNotTimestamp, Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {
    final AtomicInteger iRaw = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isNotTimestamp)
        .filter(isCustom)
        .filter(f -> SType.RAW.equals(colIdSTypeMap.get(f.getColId())))
        .forEach(cProfile -> mapping.put(cProfile.getColId(), iRaw.getAndAdd(1)));
  }

  public void fillMappingRaw(List<CProfile> cProfiles, List<Integer> mapping,
      Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isRaw).filter(isCustom)
        .forEach(e -> mapping.add(iRawDataLongMapping.getAndAdd(1), e.getColId()));
  }

  public String[] getStringArrayValue(RawDAO rawDAO, CType cType, byte tableId, long blockId, int colId) {
    if (CType.INT == cType) {
      return Arrays.stream(rawDAO.getRawInt(tableId, blockId, colId))
          .mapToObj(val -> val == INT_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.LONG == cType) {
      return Arrays.stream(rawDAO.getRawLong(tableId, blockId, colId))
          .mapToObj(val -> val == LONG_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.FLOAT == cType) {
      float[] floats = rawDAO.getRawFloat(tableId, blockId, colId);
      return IntStream.range(0, floats.length)
          .mapToDouble(i -> floats[i])
          .mapToObj(val -> val == FLOAT_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.DOUBLE == cType) {
      return Arrays.stream(rawDAO.getRawDouble(tableId, blockId, colId))
          .mapToObj(val -> val == DOUBLE_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.STRING == cType) {
      return Stream.of(rawDAO.getRawString(tableId, blockId, colId))
          .map(val -> val == null ? "" : val)
          .toArray(String[]::new);
    }

    return new String[0];
  }

  public static <T> List<List<T>> transpose(List<List<T>> table) {
    List<List<T>> ret = new ArrayList<List<T>>();
    final int N = table.stream().mapToInt(List::size).max().orElse(-1);
    Iterator[] iters = new Iterator[table.size()];

    int i = 0;
    for (List<T> col : table) {
      iters[i++] = col.iterator();
    }

    for (i = 0; i < N; i++) {
      List<T> col = new ArrayList<T>(iters.length);
      for (Iterator it : iters) {
        col.add(it.hasNext() ? (T) it.next() : null);
      }
      ret.add(col);
    }
    return ret;
  }

}
