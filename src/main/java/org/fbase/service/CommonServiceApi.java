package org.fbase.service;

import static org.fbase.metadata.DataType.ARRAY;
import static org.fbase.metadata.DataType.MAP;
import static org.fbase.service.mapping.Mapper.DOUBLE_NULL;
import static org.fbase.service.mapping.Mapper.FLOAT_NULL;
import static org.fbase.service.mapping.Mapper.INT_NULL;
import static org.fbase.service.mapping.Mapper.LONG_NULL;
import static org.fbase.service.mapping.Mapper.STRING_NULL;
import static org.fbase.util.MapArrayUtil.parseStringToTypedArray;
import static org.fbase.util.MapArrayUtil.parseStringToTypedMap;

import java.nio.FloatBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.fbase.model.CompareFunction;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.service.mapping.Mapper;
import org.fbase.service.store.HEntry;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.fbase.storage.helper.EnumHelper;
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

  protected int getHistogramValue(int iR,
                                  int[][] histogram,
                                  long[] timestamps) {
    int curValue = 0;

    for (int i = 0; i < histogram[0].length; i++) {
      int curIndex = histogram[0][i];
      int nextIndex;

      curValue = histogram[1][i];

      if (histogram[0].length != i + 1) {
        nextIndex = histogram[0][i + 1];

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

  public byte getTableId(String tableName,
                         MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getTableId();
  }

  public TType getTableType(String tableName,
                            MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getTableType();
  }

  public IType getIndexType(String tableName,
                            MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getIndexType();
  }

  public BType getBackendType(String tableName,
                              MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getBackendType();
  }

  public Boolean getTableCompression(String tableName,
                                     MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getCompression();
  }

  public List<CProfile> getCProfiles(String tableName,
                                     MetaModel metaModel) {
    return metaModel.getMetadata().get(tableName).getCProfiles();
  }

  public CProfile getTimestampProfile(List<CProfile> cProfileList) {
    return cProfileList.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny()
        .orElseThrow(() -> new RuntimeException("Not found timestamp column"));
  }

  protected <T, V> void setMapValue(Map<T, Map<V, Integer>> map,
                                    T vFirst,
                                    V vSecond,
                                    int sum) {
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

  protected void setMapValueEnumBlock(Map<Integer, Map<Integer, Integer>> map,
                                      Integer vFirst,
                                      int vSecond,
                                      int sum) {
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

  protected void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map,
                                         String vFirst,
                                         int vSecond,
                                         int sum) {
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

  protected void setMapValueRawEnumBlock(Map<Integer, Map<String, Integer>> map,
                                         int vFirst,
                                         String vSecond,
                                         int sum) {
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

  protected <T> void fillArrayList(List<List<T>> array,
                                   int colCount) {
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
          () -> FloatBuffer.allocate(row.size()),
          FloatBuffer::put,
          (left, right) -> {
            throw new UnsupportedOperationException("Only called in parallel stream");
          }).array();
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

  public int[] getIntegerFromSet(Set<Integer> list) {
    int[] values = new int[list.size()];
    int index = 0;
    for (Integer key : list) {
      values[index++] = key;
    }
    return values;
  }

  public String[] getStringFromList(List<String> list) {
    String[] stringArray = new String[list.size()];
    int index = 0;
    for (String b : list) {
      stringArray[index++] = b;
    }
    return stringArray;
  }

  public void fillTimestampMap(List<CProfile> cProfiles,
                               CachedLastLinkedHashMap<Integer, Integer> mapping) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(f -> f.getCsType().isTimeStamp())
        .forEach(e -> mapping.put(e.getColId(), iRawDataLongMapping.getAndAdd(1)));
  }

  public void fillAllEnumMappingSType(List<CProfile> cProfiles,
                                      CachedLastLinkedHashMap<Integer, Integer> mapping,
                                      List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEnumEColumn,
                                      Map<Integer, SType> colIdSTypeMap) {

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
                             CachedLastLinkedHashMap<Integer, Integer> mapping,
                             Map<Integer, SType> colIdSTypeMap,
                             Predicate<CProfile> isNotTimestamp,
                             Predicate<CProfile> isRaw,
                             Predicate<CProfile> isCustom) {
    final AtomicInteger iRaw = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isNotTimestamp)
        .filter(isCustom)
        .filter(f -> SType.RAW.equals(colIdSTypeMap.get(f.getColId())))
        .forEach(cProfile -> mapping.put(cProfile.getColId(), iRaw.getAndAdd(1)));
  }

  public void fillMappingRaw(List<CProfile> cProfiles,
                             List<Integer> mapping,
                             Predicate<CProfile> isRaw,
                             Predicate<CProfile> isCustom) {
    final AtomicInteger iRawDataLongMapping = new AtomicInteger(0);

    cProfiles.stream()
        .filter(isRaw).filter(isCustom)
        .forEach(e -> mapping.add(iRawDataLongMapping.getAndAdd(1), e.getColId()));
  }

  protected String[] getStringArrayValues(RawDAO rawDAO,
                                          EnumDAO enumDAO,
                                          HistogramDAO histogramDAO,
                                          Converter converter,
                                          byte tableId,
                                          CProfile cProfile,
                                          long blockId,
                                          long[] timestamps) {
    MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();
    SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));

    if (SType.RAW.equals(sType)) {
      return getStringArrayValuesRaw(rawDAO, tableId, blockId, cProfile);
    } else if (SType.ENUM.equals(sType)) {
      return getStringArrayValuesEnum(enumDAO, converter, tableId, blockId, cProfile, timestamps);
    } else if (SType.HISTOGRAM.equals(sType)) {
      return getStringArrayValuesHist(histogramDAO, converter, tableId, blockId, cProfile, timestamps);
    }

    return new String[0];
  }

  public String[] getStringArrayValuesRaw(RawDAO rawDAO,
                                          byte tableId,
                                          long blockId,
                                          CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);

    if (CType.INT == cType) {
      return Arrays.stream(rawDAO.getRawInt(tableId, blockId, colId))
          .mapToObj(val -> val == INT_NULL ? "" : String.valueOf(val))
          .toArray(String[]::new);
    } else if (CType.LONG == cType) {
      return switch (cProfile.getCsType().getDType()) {
        case TIMESTAMP, TIMESTAMPTZ, DATETIME, DATETIME2, SMALLDATETIME ->
            Arrays.stream(rawDAO.getRawLong(tableId, blockId, colId))
                .mapToObj(val -> val == LONG_NULL ? "" : getDateForLongShorted(Math.toIntExact(val / 1000)))
                .toArray(String[]::new);
        default -> Arrays.stream(rawDAO.getRawLong(tableId, blockId, colId))
            .mapToObj(val -> val == LONG_NULL ? "" : String.valueOf(val))
            .toArray(String[]::new);
      };
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

  protected String[] getStringArrayValuesEnum(EnumDAO enumDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    String[] array = new String[timestamps.length];

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      byte valueByte = eColumn.getDataByte()[iR];
      array[iR] = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn.getValues(), valueByte), cProfile);
    });

    return array;
  }

  protected String[] getStringArrayValuesHist(HistogramDAO histogramDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    String[] array = new String[timestamps.length];

    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());
    int[] histogramsUnPack = getHistogramUnPack(timestamps, histograms);

    IntStream iRow = IntStream.range(0, timestamps.length);

    iRow.forEach(iR -> array[iR] = converter.convertIntToRaw(histogramsUnPack[iR], cProfile));

    return array;
  }

  protected double[] getDoubleArrayValues(RawDAO rawDAO,
                                          EnumDAO enumDAO,
                                          HistogramDAO histogramDAO,
                                          Converter converter,
                                          byte tableId,
                                          CProfile cProfile,
                                          long blockId,
                                          long[] timestamps) {
    MetadataKey metadataKey = MetadataKey.builder().tableId(tableId).blockId(blockId).build();
    SType sType = getSType(cProfile.getColId(), rawDAO.getMetadata(metadataKey));

    if (SType.RAW.equals(sType)) {
      return getDoubleArrayValuesRaw(rawDAO, tableId, blockId, cProfile);
    } else if (SType.ENUM.equals(sType)) {
      return getDoubleArrayValuesEnum(enumDAO, converter, tableId, blockId, cProfile, timestamps);
    } else if (SType.HISTOGRAM.equals(sType)) {
      return getDoubleArrayValuesHist(histogramDAO, converter, tableId, blockId, cProfile, timestamps);
    }

    return new double[0];
  }

  protected double[] getDoubleArrayValuesRaw(RawDAO rawDAO,
                                             byte tableId,
                                             long blockId,
                                             CProfile cProfile) {
    int colId = cProfile.getColId();
    CType cType = Mapper.isCType(cProfile);

    if (CType.INT == cType) {
      int[] values = rawDAO.getRawInt(tableId, blockId, colId);
      double[] result = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = values[i] == INT_NULL ? 0 : values[i];
      }
      return result;
    } else if (CType.LONG == cType) {
      long[] values = rawDAO.getRawLong(tableId, blockId, colId);
      double[] result = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = values[i] == LONG_NULL ? 0 : values[i];
      }
      return result;
    } else if (CType.FLOAT == cType) {
      float[] values = rawDAO.getRawFloat(tableId, blockId, colId);
      double[] result = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = values[i] == FLOAT_NULL ? 0 : values[i];
      }
      return result;
    } else if (CType.DOUBLE == cType) {
      double[] values = rawDAO.getRawDouble(tableId, blockId, colId);
      double[] result = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = values[i] == DOUBLE_NULL ? 0 : values[i];
      }
      return result;
    }

    return new double[0];
  }

  protected double[] getDoubleArrayValuesEnum(EnumDAO enumDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    double[] array = new double[timestamps.length];

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    IntStream.range(0, timestamps.length).forEach(iR -> {
      byte valueByte = eColumn.getDataByte()[iR];
      int intValue = EnumHelper.getIndexValue(eColumn.getValues(), valueByte);
      array[iR] = converter.convertIntToDouble(intValue, cProfile);
    });

    return array;
  }

  protected double[] getDoubleArrayValuesHist(HistogramDAO histogramDAO,
                                              Converter converter,
                                              byte tableId,
                                              long blockId,
                                              CProfile cProfile,
                                              long[] timestamps) {
    double[] array = new double[timestamps.length];

    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());
    int[] histogramsUnPack = getHistogramUnPack(timestamps, histograms);

    IntStream.range(0, timestamps.length).forEach(iR -> {
      array[iR] = converter.convertIntToDouble(histogramsUnPack[iR], cProfile);
    });

    return array;
  }

  protected <T> void fillTimeSeriesColumnData(long[] timestamps, long begin, long end, List<T> columnData) {
    if (timestamps.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          columnData.add((T) Long.valueOf(timestamps[iR]));
        }
      });
    }
  }

  protected <F, T> void fillTimeSeriesColumnDataFilter(Set<F> indexSetFilter, long[] timestamps, List<T> columnData) {
    if (timestamps.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (indexSetFilter.contains(iR)) {
          columnData.add((T) Long.valueOf(timestamps[iR]));
        }
      });
    }
  }

  protected <V, T> void fillColumnData(V[] columValues, long[] timestamps, long begin, long end, List<T> columnData) {
    if (columValues.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          columnData.add((T) columValues[iR]);
        }
      });
    }
  }

  protected <V, F, T> void fillColumnDataFilter(V[] columValues, Set<F> indexSetFilter, long[] timestamps, List<T> columnData) {
    if (columValues.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (indexSetFilter.contains(iR)) {
          columnData.add((T) columValues[iR]);
        }
      });
    }
  }

  protected <V, T> void fillColumnDataSet(V[] columValues, long[] timestamps, long begin, long end, Set<T> columnData) {
    if (columValues.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin && timestamps[iR] <= end) {
          columnData.add((T) columValues[iR]);
        }
      });
    }
  }

  protected <I, V> Set<I> getIndexSetByFilter(V[] columValues,
                                              CProfile cProfileFilter,
                                              String filter,
                                              long[] timestamps,
                                              long begin,
                                              long end) {
    if (cProfileFilter.getCsType().isTimeStamp()) {
      throw new RuntimeException("Not supported for timestamp");
    }

    Set<I> setIndexRow = new HashSet<>();

    if (timestamps.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          if (filter.equals(columValues[iR])) {
            setIndexRow.add((I) Integer.valueOf(iR));
          }
        }
      });
    }

    return setIndexRow;
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

  protected SType getSType(int colId,
                           Metadata metadata) {
    IntPredicate colIdPredicate = (x) -> x == colId;

    if (Arrays.stream(metadata.getRawColIds()).anyMatch(colIdPredicate)) {
      return SType.RAW;
    } else if (Arrays.stream(metadata.getEnumColIds()).anyMatch(colIdPredicate)) {
      return SType.ENUM;
    } else if (Arrays.stream(metadata.getHistogramColIds()).anyMatch(colIdPredicate)) {
      return SType.HISTOGRAM;
    }

    throw new RuntimeException("Undefined storage type for column id: " + colId);
  }

  protected List<StackedColumn> handleMap(List<StackedColumn> sColumnList) {
    List<StackedColumn> sColumnListParsedMap = new ArrayList<>();

    sColumnList.forEach(stackedColumn -> {

      Map<String, Integer> keyCount = new HashMap<>();
      stackedColumn.getKeyCount().forEach((key, value) -> {
        Map<String, Long> parsedMap = parseStringToTypedMap(
            key,
            String::new,
            Long::parseLong,
            "="
        );

        for (Entry<String, Long> pair : parsedMap.entrySet()) {
          Long newCount = (pair.getValue() == null) ? 0 : pair.getValue() * value;
          pair.setValue(newCount);
        }

        parsedMap.forEach((keyParsed, valueParsed) ->
                              keyCount.merge(keyParsed, Math.toIntExact(valueParsed), Integer::sum));
      });

      sColumnListParsedMap.add(StackedColumn.builder()
                                   .key(stackedColumn.getKey())
                                   .tail(stackedColumn.getTail())
                                   .keyCount(keyCount).build());
    });

    return sColumnListParsedMap;
  }


  protected List<StackedColumn> handleArray(List<StackedColumn> sColumnList) {
    List<StackedColumn> sColumnListParsedMap = new ArrayList<>();

    sColumnList.forEach(stackedColumn -> {

      Map<String, Integer> keyCount = new HashMap<>();
      stackedColumn.getKeyCount().forEach((key, value) -> {
        String[] array = parseStringToTypedArray(key, ",");

        Arrays.stream(array).forEach(e -> keyCount.merge(e.trim(), value, Integer::sum));

        sColumnListParsedMap.add(StackedColumn.builder()
                                     .key(stackedColumn.getKey())
                                     .tail(stackedColumn.getTail())
                                     .keyCount(keyCount).build());
      });
    });

    return sColumnListParsedMap;
  }

  protected List<GanttColumnCount> handleMap(CProfile firstLevelGroupBy,
                                             CProfile secondLevelGroupBy,
                                             Map<String, Map<String, Integer>> mapFinalIn) {
    List<GanttColumnCount> list = new ArrayList<>();
    Map<String, Map<String, Integer>> mapFinalOut = new HashMap<>();

    if (MAP.equals(firstLevelGroupBy.getCsType().getDType())) {
      handlerFirstLevelMap(mapFinalIn, mapFinalOut);
    }

    if (MAP.equals(secondLevelGroupBy.getCsType().getDType())) {
      if (MAP.equals(firstLevelGroupBy.getCsType().getDType())) {
        Map<String, Map<String, Integer>> updates = new HashMap<>();

        handlerSecondLevelMap(mapFinalOut, updates);

        mapFinalOut.clear();

        updates.forEach((key, value) -> {
          value.forEach((updateKey, updateValue) -> setMapValue(mapFinalOut, key, updateKey, updateValue));
        });
      } else {
        handlerSecondLevelMap(mapFinalIn, mapFinalOut);
      }
    }

    mapFinalOut.forEach((key, value) -> list.add(GanttColumnCount.builder().key(key).gantt(value).build()));

    return list;
  }

  private void handlerFirstLevelMap(Map<String, Map<String, Integer>> mapFinalIn,
                                    Map<String, Map<String, Integer>> mapFinalOut) {
    mapFinalIn.forEach((kIn, vIn) -> {
      Map<String, Long> parsedMap = parsedMap(kIn);

      if (parsedMap.isEmpty()) {
        vIn.forEach((kvIn, vvIn) -> setMapValue(mapFinalOut, STRING_NULL, kvIn, vvIn));
      }

      parsedMap.forEach((kP, vP) -> vIn.forEach((kvIn, vvIn) -> setMapValue(mapFinalOut, kP, kvIn,
                                                                            Math.toIntExact(vP) * vvIn)));
    });
  }

  private void handlerSecondLevelMap(Map<String, Map<String, Integer>> mapFinalIn,
                                     Map<String, Map<String, Integer>> mapFinalOut) {
    mapFinalIn.forEach((kIn, vIn) -> vIn.forEach((kvIn, vvIn) -> {
      Map<String, Long> parsedMap = parsedMap(kvIn);

      if (parsedMap.isEmpty()) {
        setMapValue(mapFinalOut, kIn, STRING_NULL, vvIn);
      }

      parsedMap.forEach((kP, vP) -> setMapValue(mapFinalOut, kIn, kP, Math.toIntExact(vP) * vvIn));
    }));
  }

  private Map<String, Long> parsedMap(String input) {
    return parseStringToTypedMap(
        input,
        String::new,
        Long::parseLong,
        "="
    );
  }

  protected Map<String, Map<String, Integer>> handleArray(CProfile firstLevelGroupBy,
                                                          CProfile secondLevelGroupBy,
                                                          Map<String, Map<String, Integer>> mapFinalIn) {
    Map<String, Map<String, Integer>> mapFinalOut = new HashMap<>();

    if (ARRAY.equals(firstLevelGroupBy.getCsType().getDType())) {
      handlerFirstLevelArray(mapFinalIn, mapFinalOut);
    }

    if (ARRAY.equals(secondLevelGroupBy.getCsType().getDType())) {
      if (ARRAY.equals(firstLevelGroupBy.getCsType().getDType())) {
        Map<String, Map<String, Integer>> updates = new HashMap<>();

        handlerSecondLevelArray(mapFinalOut, updates);

        mapFinalOut.clear();

        updates.forEach((key, value) -> {
          value.forEach((updateKey, updateValue) -> setMapValue(mapFinalOut, key, updateKey, updateValue));
        });
      } else {
        handlerSecondLevelArray(mapFinalIn, mapFinalOut);
      }
    }

    return mapFinalOut;
  }

  private void handlerFirstLevelArray(Map<String, Map<String, Integer>> mapFinalIn,
                                      Map<String, Map<String, Integer>> mapFinalOut) {
    mapFinalIn.forEach((kIn, vIn) -> {
      String[] array = parseStringToTypedArray(kIn, ",");

      if (array.length == 0) {
        vIn.forEach((kvIn, vvIn) -> setMapValue(mapFinalOut, STRING_NULL, kvIn, vvIn));
      }

      for (int i = 0; i < array.length; i++) {
        int finalI = i;
        vIn.forEach((kvIn, vvIn) -> setMapValue(mapFinalOut, array[finalI].trim(), kvIn, vvIn));
      }
    });
  }

  private void handlerSecondLevelArray(Map<String, Map<String, Integer>> mapFinalIn,
                                       Map<String, Map<String, Integer>> mapFinalOut) {
    mapFinalIn.forEach((kIn, vIn) -> vIn.forEach((kvIn, vvIn) -> {
      String[] array = parseStringToTypedArray(kvIn, ",");

      if (array.length == 0) {
        setMapValue(mapFinalOut, kIn, STRING_NULL, vvIn);
      }

      for (int i = 0; i < array.length; i++) {
        int finalI = i;
        setMapValue(mapFinalOut, kIn, array[finalI], vvIn);
      }
    }));
  }

  protected int[] getHistogramUnPack(long[] timestamps,
                                     int[][] histograms) {
    AtomicInteger cntForHist = new AtomicInteger(0);

    int[] histogramsUnPack = new int[timestamps.length];

    AtomicInteger cnt = new AtomicInteger(0);
    for (int i = 0; i < histograms[0].length; i++) {
      if (histograms[0].length != 1) {
        int deltaValue = 0;
        int currValue = histograms[0][cnt.getAndIncrement()];
        int currHistogramValue = histograms[1][cnt.get() - 1];

        if (currValue == timestamps.length - 1) {
          deltaValue = 1;
        } else { // not
          if (histograms[0].length == cnt.get()) {// last value abs
            int nextValue = timestamps.length;
            deltaValue = nextValue - currValue;
          } else {
            int nextValue = histograms[0][cnt.get()];
            deltaValue = nextValue - currValue;
          }
        }

        IntStream iRow = IntStream.range(0, deltaValue);
        iRow.forEach(iR -> histogramsUnPack[cntForHist.getAndIncrement()] = currHistogramValue);
      } else {
        for (int j = 0; j < timestamps.length; j++) {
          histogramsUnPack[j] = histograms[1][0]; // Fix for BUG in batch jdbc insert when block contains empty values
        }
      }
    }

    return histogramsUnPack;
  }

  protected Map.Entry<MetadataKey, MetadataKey> getMetadataKeyPair(byte tableId,
                                                                   long begin,
                                                                   long end,
                                                                   long previousBlockId) {
    MetadataKey beginMKey;
    MetadataKey endMKey;

    if (previousBlockId != begin & previousBlockId != 0) {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(previousBlockId).build();
    } else {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    }
    endMKey = MetadataKey.builder().tableId(tableId).blockId(end).build();

    return new SimpleImmutableEntry<>(beginMKey, endMKey);
  }

  protected int getNextIndex(int i,
                           int[][] histogram,
                           long[] timestamps) {
    int nextIndex;

    if (i + 1 < histogram[0].length) {
      nextIndex = histogram[0][i + 1] - 1;
    } else {
      nextIndex = timestamps.length - 1;
    }

    return nextIndex;
  }

  private String getDateForLongShorted(int longDate) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    Date dtDate = new Date(((long) longDate) * 1000L);
    return simpleDateFormat.format(dtDate);
  }

  protected boolean filter(String filterValue, String[] filterData, CompareFunction compareFunction) {
    if (compareFunction == null || filterData == null || filterValue == null || filterData.length == 0) return true;

    boolean result;
    switch (compareFunction) {
      case EQUAL -> result = Arrays.asList(filterData).contains(filterValue);
      case CONTAIN -> result = Arrays.stream(filterData)
          .map(String::toLowerCase)
          .anyMatch(filter -> filterValue.toLowerCase().contains(filter));
      default -> throw new IllegalArgumentException("Unsupported CompareFunction: " + compareFunction);
    }

    return result;
  }

  protected LocalDateTime toLocalDateTime(long ofEpochMilli) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(ofEpochMilli), TimeZone.getDefault().toZoneId());
  }
}
