package org.fbase.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.fbase.core.Converter;
import org.fbase.core.Mapper;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByService;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.helper.EnumHelper;

public class GroupByServiceImpl extends CommonServiceApi implements GroupByService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final MetadataDAO metadataDAO;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByServiceImpl(MetaModel metaModel, Converter converter, MetadataDAO metadataDAO,
      HistogramDAO histogramDAO, RawDAO rawDAO, EnumDAO enumDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.metadataDAO = metadataDAO;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<GanttColumn> getListGanttColumnUniversal(String tableName, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end) {

    byte tableId = getTableId(tableName, metaModel);

    int tsColId = getCProfiles(tableName, metaModel).stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findFirst()
        .orElseThrow()
        .getColId();

    List<GanttColumn> list = new ArrayList<>();

    if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.ENUM)) {
      enumEnumBlock(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.RAW)) {
      rawRaw(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.HISTOGRAM, SType.ENUM)) {
      histEnumBlock(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.HISTOGRAM)) {
      enumHistBlock(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.HISTOGRAM, SType.RAW)) {
      histRaw(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.HISTOGRAM)) {
      rawHist(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.RAW)) {
      enumRawBlock(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.ENUM)) {
      rawEnumBlock(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    }

    return list;
  }

  public void enumEnumBlock(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumEnumBlock(tableId, tsColId, prevKey, firstLevelGroupBy, secondLevelGroupBy,
          begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeEnumEnumBlock(tableId, tsColId, key,
            firstLevelGroupBy, secondLevelGroupBy, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
        .key(this.converter.convertIntToRaw(key, firstLevelGroupBy))
        .gantt(getEnumBlockMap(value, secondLevelGroupBy)).build()));
  }

  private void computeEnumEnumBlock(byte tableId, int tsColId, long key,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end,
      Map<Integer, Map<Integer, Integer>> map) {

    Map.Entry<int[], byte[]> listFirst = computeEnumEnumBlock(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumEnumBlock(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueEnumEnumBlock(map, listFirst, listSecond, 1);
  }

  public void rawRaw(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawRaw(tableId, firstLevelGroupBy, secondLevelGroupBy, tsColId,
          prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeRawRaw(tableId, firstLevelGroupBy, secondLevelGroupBy, tsColId, key, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder().key(key).gantt(value).build()));
  }

  private void computeRawRaw(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<String, Map<String, Integer>> map) {

    long[] timestamps = rawDAO.getRawLong(tableId, key, tsColId);

    String[] first = getStringArrayValue(rawDAO, Mapper.isCType(firstLevelGroupBy),
        tableId, key, firstLevelGroupBy.getColId());
    String[] second = getStringArrayValue(rawDAO, Mapper.isCType(secondLevelGroupBy),
        tableId, key, secondLevelGroupBy.getColId());

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        setMapValue(map, first[iR], second[iR], 1);
      }
    });
  }

  public void histEnumBlock(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeHistEnumBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeHistEnumBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
                tsColId, key, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
        .key(this.converter.convertIntToRaw(key, firstLevelGroupBy))
        .gantt(getEnumBlockMap(value, secondLevelGroupBy)).build()));
  }

  private void computeHistEnumBlock(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<Integer, Map<Integer, Integer>> map) {

    List<Integer> listFirst = computeHistogram(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumBlock(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueHistEnumBlock(map, listFirst, listSecond, 1);
  }

  private List<Integer> computeHistogram(byte tableId, CProfile cProfile,
      int tsColId, long key, long begin, long end) {

    List<Integer> list = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, key, tsColId);

    int[][] h = histogramDAO.get(metadataDAO.getHistograms(tableId, key)[cProfile.getColId()]);

    for (int i = 0; i < h.length; i++) {
      int fNextIndex = getNextIndex(i, h, timestamps);
      int startIndex;

      if (i == 0) {
        startIndex = 0;
      } else {
        startIndex = fNextIndex - (fNextIndex - getNextIndex(i - 1, h, timestamps)) + 1;
      }

      for (int k = startIndex; k <= fNextIndex; k++) {
        boolean checkRange = timestamps[k] >= begin & timestamps[k] <= end;
        if (checkRange) {
          list.add(h[i][1]);
        }
      }
    }

    return list;
  }

  private Map.Entry<int[], byte[]> computeEnumBlock(byte tableId, CProfile cProfile,
      int tsColId, long key, long begin, long end) {

    long[] tsSecond = rawDAO.getRawLong(tableId, key, tsColId);
    byte[] eBytes = new byte[tsSecond.length];

    byte[] bytes = this.rawDAO.getRawByte(tableId, key, cProfile.getColId());

    IntStream iRow = IntStream.range(0, tsSecond.length);

    iRow.forEach(iR -> {
      if (tsSecond[iR] >= begin & tsSecond[iR] <= end) {
        eBytes[iR] = bytes[iR];
      }
    });

    int[] eColumn = enumDAO.getEColumnValues(tableId, key, cProfile.getColId());

    return Map.entry(eColumn, eBytes);
  }

  private Map.Entry<int[], byte[]> computeEnumEnumBlock(byte tableId, CProfile cProfile,
      int tsColId, long key, long begin, long end) {

    long[] tsSecond = rawDAO.getRawLong(tableId, key, tsColId);
    List<Byte> eBytes = new ArrayList<>();

    byte[] bytes = this.rawDAO.getRawByte(tableId, key, cProfile.getColId());

    IntStream iRow = IntStream.range(0, tsSecond.length);

    iRow.forEach(iR -> {
      if (tsSecond[iR] >= begin & tsSecond[iR] <= end) {
        eBytes.add(bytes[iR]);
      }
    });

    int[] eColumn = enumDAO.getEColumnValues(tableId, key, cProfile.getColId());

    return Map.entry(eColumn, getByteFromList(eBytes));
  }

  private List<String> computeRaw(byte tableId, CProfile cProfile,
      int tsColId, long key, long begin, long end) {

    List<String> list = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, key, tsColId);

    String[] columnData = getStringArrayValue(rawDAO, Mapper.isCType(cProfile),
        tableId, key, cProfile.getColId());

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        list.add(columnData[iR]);
      }
    });

    return list;
  }

  public void enumHistBlock(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumHistBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeEnumHistBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
            tsColId, key, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
        .key(this.converter.convertIntToRaw(key, firstLevelGroupBy))
        .gantt(getHistogramGanttMap(value, secondLevelGroupBy)).build()));
  }

  private void computeEnumHistBlock(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<Integer, Map<Integer, Integer>> map) {

    Map.Entry<int[], byte[]> listFirst = computeEnumEnumBlock(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueCommonBlockLevel(map, listFirst, listSecond, 1);
  }

  public void histRaw(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeHistRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId,  prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key ->  this.computeHistRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
            tsColId, key, begin, end, map));

    map.forEach((key, value) -> {
      String keyStr = this.converter.convertIntToRaw(key, firstLevelGroupBy);
      list.add(GanttColumn.builder().key(keyStr).gantt(value).build());
    });
  }

  private void computeHistRaw(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<Integer, Map<String, Integer>> map) {

    List<Integer> listFirst = computeHistogram(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    List<String> listSecond = computeRaw(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void rawHist(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key ->  this.computeRawHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, key, begin, end, map));

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder().key(key).gantt(getHistogramGanttMap(value, secondLevelGroupBy)).build());
    });
  }

  private void computeRawHist(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<String, Map<Integer, Integer>> map) {

    List<String> listFirst = computeRaw(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void enumRawBlock(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumRawBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeEnumRawBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, key, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
        .key(this.converter.convertIntToRaw(key, firstLevelGroupBy))
        .gantt(value).build()));
  }

  private void computeEnumRawBlock(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<Integer, Map<String, Integer>> map) {

    Map.Entry<int[], byte[]> listFirst = computeEnumEnumBlock(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    List<String> listSecond = computeRaw(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueEnumRawBlock(map, listFirst, listSecond, 1);
  }

  public void rawEnumBlock(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawEnumBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, prevKey, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> this.computeRawEnumBlock(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, key, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
        .key(key)
        .gantt(getEnumBlockMap(value, secondLevelGroupBy)).build()));
  }

  private void computeRawEnumBlock(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, long key, long begin, long end,
      Map<String, Map<Integer, Integer>> map) {

    List<String> listFirst = computeRaw(tableId, firstLevelGroupBy, tsColId, key, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumBlock(tableId, secondLevelGroupBy, tsColId, key, begin, end);

    setMapValueRawEnumBlock(map, listFirst, listSecond, 1);
  }

  private int getNextIndex(int i, int[][] array, long[] timestamps) {
    int nextIndex;

    if (i + 1 < array.length) {
      nextIndex = array[i + 1][0] - 1;
    } else {
      nextIndex = timestamps.length - 1;
    }

    return nextIndex;
  }

  private boolean checkSType(CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, SType firstSType,
      SType secondSType) {
    return firstLevelGroupBy.getCsType().getSType().equals(firstSType) &
        secondLevelGroupBy.getCsType().getSType().equals(secondSType);
  }

  private <T, V> void setMapValueCommon(Map<T, Map<V, Integer>> map, List<T> listFirst, List<V> listSecond,
      int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      setMapValue(map, listFirst.get(i), listSecond.get(i), sum);
    }
  }

  private void setMapValueEnumEnumBlock(Map<Integer, Map<Integer, Integer>> map, Map.Entry<int[], byte[]> entryFirst,
      Map.Entry<int[], byte[]> entrySecond, int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      int intToRawSecond = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueEnumBlock(map, intToRawFirst, intToRawSecond, sum);
    }
  }

  private void setMapValueHistEnumBlock(Map<Integer, Map<Integer, Integer>> map, List<Integer> listFirst,
      Map.Entry<int[], byte[]> entrySecond, int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      int intToRaw = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueEnumBlock(map, listFirst.get(i), intToRaw, sum);
    }
  }

  private void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map, List<String> listFirst,
      Map.Entry<int[], byte[]> entrySecond, int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      int intToRaw = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueRawEnumBlock(map, listFirst.get(i), intToRaw, sum);
    }
  }

  private void setMapValueCommonBlockLevel(Map<Integer, Map<Integer, Integer>> map,
      Map.Entry<int[], byte[]> entryFirst, List<Integer> listSecond, int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      setMapValueEnumBlock(map, intToRawFirst, listSecond.get(i), sum);
    }
  }

  private void setMapValueEnumRawBlock(Map<Integer, Map<String, Integer>> map,
      Map.Entry<int[], byte[]> entryFirst, List<String> listSecond, int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      setMapValueRawEnumBlock(map, intToRawFirst, listSecond.get(i), sum);
    }
  }

  private Map<String, Integer> getHistogramGanttMap(Map<Integer, Integer> value,
      CProfile secondLevelGroupBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> this.converter.convertIntToRaw(k.getKey(), secondLevelGroupBy),
            Map.Entry::getValue));
  }

  private Map<String, Integer> getEnumBlockMap(Map<Integer, Integer> value, CProfile secondLevelGroupBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> converter.convertIntToRaw(k.getKey(), secondLevelGroupBy), Map.Entry::getValue));
  }
}