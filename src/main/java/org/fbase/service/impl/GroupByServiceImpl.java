package org.fbase.service.impl;

import org.fbase.core.Converter;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByService;
import org.fbase.service.container.RawContainer;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.dto.MetadataDto;
import org.fbase.storage.helper.EnumHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public List<GanttColumn> getListGanttColumnUniversal(TProfile tProfile, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end) {

    byte tableId = getTableId(tProfile, metaModel);

    int tsColId = getCProfiles(tProfile, metaModel).stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findFirst()
        .orElseThrow()
        .getColId();

    List<GanttColumn> list = new ArrayList<>();

    if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.ENUM)) {
      enumEnum(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.RAW)) {
      rawRaw(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.HISTOGRAM, SType.ENUM)) {
      histEnum(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.HISTOGRAM)) {
      enumHist(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.HISTOGRAM, SType.RAW)) {
      histRaw(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.HISTOGRAM)) {
      rawHist(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.ENUM, SType.RAW)) {
      enumRaw(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    } else if (checkSType(firstLevelGroupBy, secondLevelGroupBy, SType.RAW, SType.ENUM)) {
      rawEnum(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy, begin, end, list);
    }

    return list;
  }

  public void enumEnum(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Byte, Map<Byte, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumEnum(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy,
          this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeEnumEnum(tableId, tsColId, firstLevelGroupBy, secondLevelGroupBy,
          md, begin, end, map);
    }

    int[] eColumnFirst = enumDAO.getEColumnValues(tableId, firstLevelGroupBy.getColId());
    int[] eColumnSecond = enumDAO.getEColumnValues(tableId, secondLevelGroupBy.getColId());

    map.forEach((key, value) -> {
      String keyStr = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumnFirst, key),
          firstLevelGroupBy);
      list.add(
          GanttColumn.builder().key(keyStr).gantt(getRawEnumMap(eColumnSecond, value, secondLevelGroupBy))
              .build());
    });
  }

  private void computeEnumEnum(byte tableId, int tsColId, CProfile firstLevelGroupBy, CProfile secondLevelGroupBy,
      MetadataDto mdto, long begin, long end, Map<Byte, Map<Byte, Integer>> map) {

    long[] timestamps = rawDAO.getRawLong(tableId, mdto.getKey(), tsColId);

    RawContainer rcFirst = new RawContainer(mdto.getKey(), firstLevelGroupBy,
        this.rawDAO.getRawData(tableId, mdto.getKey(), firstLevelGroupBy.getColId()));
    RawContainer rcSecond = new RawContainer(mdto.getKey(), secondLevelGroupBy,
        this.rawDAO.getRawData(tableId, mdto.getKey(), secondLevelGroupBy.getColId()));

    IntStream iRow = IntStream.range(0, timestamps.length);

    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        setMapValue(map, rcFirst.getEnumValueForCell(iR), rcSecond.getEnumValueForCell(iR), 1);
      }
    });
  }

  public void rawRaw(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawRaw(tableId, firstLevelGroupBy, secondLevelGroupBy, tsColId,
          this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeRawRaw(tableId, firstLevelGroupBy, secondLevelGroupBy, tsColId, md, begin,
          end, map);
    }

    map.forEach((key, value) -> list.add(GanttColumn.builder().key(key).gantt(value).build()));
  }

  private void computeRawRaw(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<String, Map<String, Integer>> map) {

    long[] timestamps = rawDAO.getRawLong(tableId, mdto.getKey(), tsColId);

    RawContainer rcFirst = new RawContainer(mdto.getKey(), firstLevelGroupBy,
        this.rawDAO.getRawData(tableId, mdto.getKey(), firstLevelGroupBy.getColId()));
    RawContainer rcSecond = new RawContainer(mdto.getKey(), secondLevelGroupBy,
        this.rawDAO.getRawData(tableId, mdto.getKey(), secondLevelGroupBy.getColId()));

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        setMapValue(map, rcFirst.getStrValueForCell(iR), rcSecond.getStrValueForCell(iR), 1);
      }
    });
  }

  public void histEnum(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<Byte, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeHistEnum(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeHistEnum(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    int[] eColumnSecond = enumDAO.getEColumnValues(tableId, secondLevelGroupBy.getColId());

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder().key(this.converter.convertIntToRaw(key, firstLevelGroupBy))
          .gantt(getHistogramEnumMap(eColumnSecond, value, secondLevelGroupBy)).build());
    });
  }

  private void computeHistEnum(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<Integer, Map<Byte, Integer>> map) {

    List<Integer> listFirst = computeHistogram(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<Byte> listSecond = computeEnum(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  private List<Integer> computeHistogram(byte tableId, CProfile cProfile,
      int tsColId, MetadataDto mdto, long begin, long end) {

    List<Integer> list = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, mdto.getKey(), tsColId);

    int[][] h = histogramDAO.get(mdto.getHistograms()[cProfile.getColId()]);

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

  private List<Byte> computeEnum(byte tableId, CProfile cProfile,
      int tsColId, MetadataDto mdto, long begin, long end) {

    List<Byte> list = new ArrayList<>();

    long[] tsSecond = rawDAO.getRawLong(tableId, mdto.getKey(), tsColId);

    RawContainer rawContainer =
        new RawContainer(mdto.getKey(), cProfile,
            this.rawDAO.getRawData(tableId, mdto.getKey(), cProfile.getColId()));

    IntStream iRow = IntStream.range(0, tsSecond.length);

    iRow.forEach(iR -> {
      if (tsSecond[iR] >= begin & tsSecond[iR] <= end) {
        list.add(rawContainer.getEnumValueForCell(iR));
      }
    });

    return list;
  }

  private List<String> computeRaw(byte tableId, CProfile cProfile,
      int tsColId, MetadataDto mdto, long begin, long end) {

    List<String> list = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, mdto.getKey(), tsColId);

    RawContainer rawContainer = new RawContainer(mdto.getKey(), cProfile,
        this.rawDAO.getRawData(tableId, mdto.getKey(), cProfile.getColId()));

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        list.add(rawContainer.getStrValueForCell(iR));
      }
    });

    return list;
  }

  public void enumHist(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Byte, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeEnumHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    int[] eColumnFirst = enumDAO.getEColumnValues(tableId, firstLevelGroupBy.getColId());

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder()
          .key(this.converter.convertIntToRaw(EnumHelper.getIndexValue(eColumnFirst, key), firstLevelGroupBy))
          .gantt(getHistogramGanttMap(value, secondLevelGroupBy)).build());
    });
  }

  private void computeEnumHist(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<Byte, Map<Integer, Integer>> map) {

    List<Byte> listFirst = computeEnum(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void histRaw(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Integer, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeHistRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeHistRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    map.forEach((key, value) -> {
      String keyStr = this.converter.convertIntToRaw(key, firstLevelGroupBy);
      list.add(GanttColumn.builder().key(keyStr).gantt(value).build());
    });
  }

  private void computeHistRaw(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<Integer, Map<String, Integer>> map) {

    List<Integer> listFirst = computeHistogram(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<String> listSecond = computeRaw(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void rawHist(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeRawHist(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder().key(key).gantt(getHistogramGanttMap(value, secondLevelGroupBy)).build());
    });
  }

  private void computeRawHist(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<String, Map<Integer, Integer>> map) {

    List<String> listFirst = computeRaw(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void enumRaw(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<Byte, Map<String, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeEnumRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeEnumRaw(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    int[] eColumnFirst = enumDAO.getEColumnValues(tableId, firstLevelGroupBy.getColId());

    map.forEach((key, value) -> {
      String keyStr = converter.convertIntToRaw(EnumHelper.getIndexValue(eColumnFirst, key),
          firstLevelGroupBy);
      list.add(GanttColumn.builder().key(keyStr).gantt(value).build());
    });
  }

  private void computeEnumRaw(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<Byte, Map<String, Integer>> map) {

    List<Byte> listFirst = computeEnum(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<String> listSecond = computeRaw(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  public void rawEnum(byte tableId, int tsColId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, long begin, long end, List<GanttColumn> list) {

    Map<String, Map<Byte, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      this.computeRawEnum(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, this.metadataDAO.getMetadata(tableId, prevKey), begin, end, map);
    }

    for (MetadataDto md : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeRawEnum(tableId, firstLevelGroupBy, secondLevelGroupBy,
          tsColId, md, begin, end, map);
    }

    int[] eColumnSecond = enumDAO.getEColumnValues(tableId, secondLevelGroupBy.getColId());

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder().key(key).gantt(getRawEnumMap(eColumnSecond, value, secondLevelGroupBy))
          .build());
    });
  }

  private void computeRawEnum(byte tableId, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, int tsColId, MetadataDto mdto, long begin, long end,
      Map<String, Map<Byte, Integer>> map) {

    List<String> listFirst = computeRaw(tableId, firstLevelGroupBy, tsColId, mdto, begin, end);
    List<Byte> listSecond = computeEnum(tableId, secondLevelGroupBy, tsColId, mdto, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  private Map<String, Integer> getRawEnumMap(int[] eColumnSecond, Map<Byte, Integer> value,
      CProfile secondLevelGroupBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(
            k -> converter.convertIntToRaw(EnumHelper.getIndexValue(eColumnSecond, k.getKey()),
                secondLevelGroupBy),
            Map.Entry::getValue));
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

  private Map<String, Integer> getHistogramGanttMap(Map<Integer, Integer> value,
      CProfile secondLevelGroupBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> this.converter.convertIntToRaw(k.getKey(), secondLevelGroupBy),
            Map.Entry::getValue));
  }

  private Map<String, Integer> getHistogramEnumMap(int[] eColumnSecond, Map<Byte, Integer> value, CProfile secondLevelGroupBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> converter
            .convertIntToRaw(EnumHelper.getIndexValue(eColumnSecond, k.getKey()), secondLevelGroupBy), Map.Entry::getValue));
  }
}