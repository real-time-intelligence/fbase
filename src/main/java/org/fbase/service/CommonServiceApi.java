package org.fbase.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;

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

  public byte getTableId(TProfile tProfile, MetaModel metaModel) {
    return metaModel.getMetadataTables().get(tProfile.getTableId()).keySet().stream().findFirst().orElseThrow();
  }

  public List<CProfile> getCProfiles(TProfile tProfile, MetaModel metaModel) {
    return metaModel.getMetadataTables().get(tProfile.getTableId()).values().stream().findFirst().orElseThrow();
  }

  public CProfile getTimestampProfile(List<CProfile> cProfileList) {
    CProfile tsProfile = new CProfile();

        cProfileList.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .forEach(e -> tsProfile.setColId(e.getColId())
            .setColIdSql(e.getColIdSql())
            .setColName(e.getColName())
            .setColDbTypeName(e.getColDbTypeName())
            .setColSizeDisplay(e.getColSizeDisplay())
            .setColSizeSqlType(e.getColSizeSqlType())
            .setCsType(e.getCsType()));

        return tsProfile;
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

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }

    return byteArray;
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
}
