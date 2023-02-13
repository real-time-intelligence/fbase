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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.storage.RawDAO;

public abstract class CommonServiceApi {
  protected int getHistogramValue(int iR, int[][] histogram, long[] timestamps) {
    int curValue = 0;

    for (int i = 0; i < histogram.length; i++) {
      int curIndex = histogram[i][0];
      int nextIndex;

      curValue = histogram[i][1];

      if (histogram.length != i+1) {
        nextIndex = histogram[i+1][0];

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

  public List<List<Integer>> from2arrayToList(int[][] histograms) {
    return Arrays.stream(histograms)
        .map(ia -> Arrays.stream(ia)
            .boxed()
            .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  public static List<List<Float>> convert2DFloatArrayToList(float[][] input) {
    List<List<Float>> result = new ArrayList<>();
    for (float[] innerArray : input) {
      List<Float> innerList = new ArrayList<>();
      for (float value : innerArray) {
        innerList.add(value);
      }
      result.add(innerList);
    }
    return result;
  }

  public static List<List<Byte>> convert2DByteArrayToList(byte[][] input) {
    List<List<Byte>> result = new ArrayList<>();
    for (byte[] innerArray : input) {
      List<Byte> innerList = new ArrayList<>();
      for (byte value : innerArray) {
        innerList.add(value);
      }
      result.add(innerList);
    }
    return result;
  }

  public int[][] getArrayFromMap(Map<Integer, Integer> map) {
    int[][] array = new int[map.size()][2];
    int count = 0;
    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
      array[count][0] = entry.getKey();
      array[count][1] = entry.getValue();
      count++;
    }
    return array;
  }

  public byte getTableId(String tableName, MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getTableId();
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
          .orElse(CProfile.builder().colId(-1).build());
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
    int[][] array = new int[rawDataInt.size()][];
    for (int i = 0; i < rawDataInt.size(); i++) {
      List<Integer> row = rawDataInt.get(i);
      array[i] = row.stream().mapToInt(j -> j).toArray();
    }
    return array;
  }

  protected long[][] getArrayLong(List<List<Long>> rawDataLong) {
    long[][] array = new long[rawDataLong.size()][];
    for (int i = 0; i < rawDataLong.size(); i++) {
      List<Long> row = rawDataLong.get(i);
      array[i] = row.stream().mapToLong(j -> j).toArray();
    }
    return array;
  }

  protected double[][] getArrayDouble(List<List<Double>> rawDataDouble) {
    double[][] array = new double[rawDataDouble.size()][];
    for (int i = 0; i < rawDataDouble.size(); i++) {
      List<Double> row = rawDataDouble.get(i);
      array[i] = row.stream().mapToDouble(j -> j).toArray();
    }
    return array;
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

  protected byte[][] getArrayByte(List<List<Byte>> rawDataByte) {
    byte[][] array = new byte[rawDataByte.size()][];
    for (int i = 0; i < rawDataByte.size(); i++) {
      List<Byte> row = rawDataByte.get(i);
      array[i] = getByteFromList(row);
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

  public void fillTimestampMapping(List<CProfile> cProfiles, List<Integer> mapping) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(f -> f.getCsType().isTimeStamp())
        .forEach(e -> mapping.add(iRawDataLongMapping.getAndAdd(1), e.getColId()));
  }

  public void fillMapping(List<CProfile> cProfiles, List<Integer> mapping,
      Predicate<CProfile> isNotTimestamp, Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isNotTimestamp).filter(isRaw).filter(isCustom)
        .forEach(e -> mapping.add(iRawDataLongMapping.getAndAdd(1), e.getColId()));
  }

  public void fillMapping(List<CProfile> cProfiles, List<Integer> mapping,
      Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isRaw).filter(isCustom)
        .forEach(e -> mapping.add(iRawDataLongMapping.getAndAdd(1), e.getColId()));
  }

  public String[][] copyOfString(String[][] rawDataString, int row) {
    String[][] rawDataStringOut = new String[rawDataString.length][];
    for(int i = 0; i < rawDataString.length; i++) {
      rawDataStringOut[i] = new String[row];
      System.arraycopy(Arrays.copyOf(rawDataString[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public byte[][] copyOfByte(byte[][] rawDataEnum, int row) {
    byte[][] rawDataStringOut = new byte[rawDataEnum.length][];
    for(int i = 0; i < rawDataEnum.length; i++) {
      rawDataStringOut[i] = new byte[row];
      System.arraycopy(Arrays.copyOf(rawDataEnum[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public int[][] copyOfInt(int[][] rawDataInt, int row) {
    int[][] rawDataStringOut = new int[rawDataInt.length][];
    for(int i = 0; i < rawDataInt.length; i++) {
      rawDataStringOut[i] = new int[row];
      System.arraycopy(Arrays.copyOf(rawDataInt[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public long[][] copyOfLong(long[][] rawDataLong, int row) {
    long[][] rawDataStringOut = new long[rawDataLong.length][];
    for(int i = 0; i < rawDataLong.length; i++) {
      rawDataStringOut[i] = new long[row];
      System.arraycopy(Arrays.copyOf(rawDataLong[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public float[][] copyOfFloat(float[][] rawDataFloat, int row) {
    float[][] rawDataStringOut = new float[rawDataFloat.length][];
    for(int i = 0; i < rawDataFloat.length; i++) {
      rawDataStringOut[i] = new float[row];
      System.arraycopy(Arrays.copyOf(rawDataFloat[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public double[][] copyOfDouble(double[][] rawDataDouble, int row) {
    double[][] rawDataStringOut = new double[rawDataDouble.length][];
    for(int i = 0; i < rawDataDouble.length; i++) {
      rawDataStringOut[i] = new double[row];
      System.arraycopy(Arrays.copyOf(rawDataDouble[i], row), 0, rawDataStringOut[i], 0, row);
    }

    return rawDataStringOut;
  }

  public String[] getStringArrayValue(RawDAO rawDAO, CType cType, byte tableId, long key, int colIndex) {
    if (CType.INT == cType) {
      return Arrays.stream(rawDAO.getRawInt(tableId, key, colIndex))
          .mapToObj(val -> val == INT_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.LONG == cType) {
      return Arrays.stream(rawDAO.getRawLong(tableId, key, colIndex))
          .mapToObj(val -> val == LONG_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.FLOAT == cType) {
      float[] floats = rawDAO.getRawFloat(tableId, key, colIndex);
      return IntStream.range(0, floats.length)
          .mapToDouble(i -> floats[i])
          .mapToObj(val -> val == FLOAT_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.DOUBLE == cType) {
      return Arrays.stream(rawDAO.getRawDouble(tableId, key, colIndex))
          .mapToObj(val -> val == DOUBLE_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.STRING == cType) {
      return Stream.of(rawDAO.getRawString(tableId, key, colIndex))
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
