package org.fbase.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.Converter;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.metadata.DataType;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.MetadataService;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;

@Log4j2
public class MetadataServiceImpl extends CommonServiceApi implements MetadataService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final MetadataDAO metadataDAO;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;

  public MetadataServiceImpl(MetaModel metaModel, Converter converter, MetadataDAO metadataDAO,
      HistogramDAO histogramDAO, RawDAO rawDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.metadataDAO = metadataDAO;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
  }

  @Override
  public List<Byte> getDataType(String tableName) {
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    return cProfiles.stream()
        .map(e -> {
          try {
            return DataType.valueOf(e.getColDbTypeName()).getKey();
          } catch (IllegalArgumentException ex) {
            log.info("ClickHouse enum data type here.. " + ex.getMessage());
            return DataType
                .valueOf(e.getColDbTypeName().substring(0, e.getColDbTypeName().indexOf("(")))
                .getKey();
          }
        }).collect(Collectors.toList());
  }

  @Override
  public List<Byte> getStorageType(String tableName) {
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    return cProfiles.stream()
        .map(e -> e.getCsType().getSType().getKey()).collect(Collectors.toList());
  }

  @Override
  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }

    return byteArray;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException {
    byte tableId = getTableId(tableName, metaModel);
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);

    if (prevKey != begin & prevKey != 0) {
      long[] timestamps = rawDAO.getRawLong(tableId, prevKey, tsProfile.getColId());
      this.computeIndexedForStackedBeginEnd(tableId, cProfile, prevKey, timestamps, begin, end, list);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> {
          long[] timestamps = rawDAO.getRawLong(tableId, key, tsProfile.getColId());

          long tail = timestamps[timestamps.length - 1];

          if (tail > end) {
            this.computeIndexedForStackedBeginEnd(tableId, cProfile, key, timestamps, begin, end, list);
          } else {
            this.computeIndexedForStackedFull(tableId, cProfile, key, timestamps, list);
          }
        });

    return list;
  }

  @Override
  public List<GanttColumn> getListGanttColumn(String tableName, CProfile firstGrpBy, CProfile secondGrpBy,
      long begin, long end) throws SqlColMetadataException {

    byte tableId = getTableId(tableName, metaModel);

    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    List<GanttColumn> list = new ArrayList<>();

    // firstLevelGroupBy = key, value = (secondLevelGroupBy = key2 : count = value2)
    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long prevKey = this.metadataDAO.getPreviousKey(tableId, begin);

    if (prevKey != begin & prevKey != 0) {
      long[] timestamps = rawDAO.getRawLong(tableId, prevKey, tsProfile.getColId());

      this.computeForGanttFull(tableId, prevKey, firstGrpBy, secondGrpBy, timestamps, begin, end, map);
    }

    this.rawDAO.getListKeys(tableId, begin, end)
        .forEach(key -> {
          long[] timestamps = rawDAO.getRawLong(tableId, key, tsProfile.getColId());
          this.computeForGanttFull(tableId, key, firstGrpBy, secondGrpBy, timestamps, begin, end, map);
        });

    this.convertMapToDto(firstGrpBy, secondGrpBy, map, list);

    return list;
  }

  @Override
  public long getLastTimestamp(String tableName, long begin, long end) {
    byte tableId = getTableId(tableName, metaModel);

    return metadataDAO.getLastTimestamp(tableId, begin, end);
  }

  private void convertMapToDto(CProfile firstGrpBy, CProfile secondGrpBy,
      Map<Integer, Map<Integer, Integer>> mapSource, List<GanttColumn> listDest) {
    mapSource.forEach((key, value) -> {
      String keyVar = this.converter.convertIntToRaw(key, firstGrpBy);

      Map<String, Integer> valueVar = new HashMap<>();
      value.forEach((k, v) -> {
        String kVar = this.converter.convertIntToRaw(k, secondGrpBy);
        valueVar.put(kVar, v);
      });

      listDest.add(GanttColumn.builder().key(keyVar).gantt(valueVar).build());
    });
  }

  private void computeIndexedForStackedFull(byte tableId, CProfile cProfile, long key, long[] timestamps,
      List<StackedColumn> list) {

    Map<Integer, Integer> map = new LinkedHashMap<>();

    long tail = timestamps[timestamps.length - 1];

    int[][] hData = histogramDAO.get(metadataDAO.getHistograms(tableId, key)[cProfile.getColId()]);

    IntStream iRow = IntStream.range(0, hData.length);
    iRow.forEach(iR -> {
      int deltaCountValue;

      if (iR == hData.length - 1) { //todo last row
        deltaCountValue = timestamps.length - hData[iR][0];
      } else {
        deltaCountValue = hData[iR + 1][0] - hData[iR][0];
      }

      map.compute(hData[iR][1], (k, val) -> val == null ? deltaCountValue : val + deltaCountValue);
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();
    map.forEach((keyInt, value) -> mapKeyCount
        .put(this.converter.convertIntToRaw(keyInt, cProfile), value));

    list.add(StackedColumn.builder()
        .key(key)
        .tail(tail)
        .keyCount(mapKeyCount).build());
  }

  private void computeIndexedForStackedBeginEnd(byte tableId, CProfile cProfile, long key,
      long[] timestamps, long begin, long end, List<StackedColumn> list) {

    long tail = timestamps[timestamps.length - 1];

    //todo replace objects with primitives
    List<List<Integer>> histogramsList =
        from2arrayToList(histogramDAO.get(metadataDAO.getHistograms(tableId, key)[cProfile.getColId()]));

    //todo replace objects with primitives
    List<Long> timestampsList =
        Arrays.stream(timestamps).boxed().collect(Collectors.toList());

    AtomicInteger cntForHistExt = new AtomicInteger(0);
    List<List<Integer>> histogramsListExt = new ArrayList<>();

    AtomicInteger cnt = new AtomicInteger(0);
    histogramsList
        .forEach(k -> {
          if (histogramsList.size() != 1) {
            int deltaValue = 0;
            int currValue = histogramsList.get(cnt.getAndIncrement()).get(0);
            int currHistogramValue = histogramsList.get(cnt.get() - 1).get(1);

            if (currValue == timestampsList.size() - 1) {
              deltaValue = 1;
            } else { // not
              if (histogramsList.size() == cnt.get()) {// last value abs
                int nextValue = timestampsList.size();
                deltaValue = nextValue - currValue;
              } else {
                int nextValue = histogramsList.get(cnt.get()).get(0);
                deltaValue = nextValue - currValue;
              }
            }

            IntStream iRow = IntStream.range(0, deltaValue);
            iRow.forEach(iR -> {
              List<Integer> obj = new ArrayList<>();
              obj.add(0, cntForHistExt.getAndIncrement());
              obj.add(1, currHistogramValue);
              histogramsListExt.add(obj);
            });

          } else {
            timestampsList.forEach(l -> {
              List<Integer> tmp = new ArrayList<>();
              tmp.add(0, l.intValue());
              tmp.add(1, histogramsList.get(0).get(1));
              histogramsListExt.add(tmp);
            });
          }
        });

    Map<String, Integer> map = new LinkedHashMap<>();
    IntStream iRow = IntStream.range(0, timestampsList.size());

    if (key < begin) {
      iRow.forEach(iR -> {
        if (timestampsList.get(iR) >= begin & timestampsList.get(iR) <= end) {
          String keyCompute = this.converter
              .convertIntToRaw(histogramsListExt.get(iR).get(1), cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (key >= begin & tail > end) {
      iRow.forEach(iR -> {
        if (timestampsList.get(iR) >= begin & timestampsList.get(iR) <= end) {

          String keyCompute = this.converter
              .convertIntToRaw(histogramsListExt.get(iR).get(1), cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    list.add(StackedColumn.builder()
        .key(key)
        .tail(tail)
        .keyCount(map).build());
  }

  private void computeForGanttFull(byte tableId, long key, CProfile firstGrpBy, CProfile secondGrpBy,
      long[] timestamps, long begin, long end, Map<Integer, Map<Integer, Integer>> map) {

    int[][] f = histogramDAO.get(metadataDAO.getHistograms(tableId, key)[firstGrpBy.getColId()]);
    int[][] l = histogramDAO.get(metadataDAO.getHistograms(tableId, key)[secondGrpBy.getColId()]);

    boolean checkRange = timestamps[f[0][0]] >= begin & timestamps[f[f.length - 1][0]] <= end;

    int lCurrent = 0;

    for (int i = 0; i < f.length; i++) {
      int fNextIndex = getNextIndex(i, f, timestamps);

      if (checkRange) {
        for (int j = lCurrent; j < l.length; j++) {
          int lNextIndex = getNextIndex(j, l, timestamps);

          if (lNextIndex <= fNextIndex) {
            if (l[j][0] <= f[i][0]) {
              setMapValue(map, f[i][1], l[j][1], (lNextIndex - f[i][0]) + 1);
            } else {
              setMapValue(map, f[i][1], l[j][1], (lNextIndex - l[j][0]) + 1);
            }
          } else {
            if (f[i][0] <= l[j][0]) {
              setMapValue(map, f[i][1], l[j][1], (fNextIndex - l[j][0]) + 1);
            } else {
              setMapValue(map, f[i][1], l[j][1], (fNextIndex - f[i][0]) + 1);
            }
          }

          if (lNextIndex > fNextIndex) {
            lCurrent = j;
            break;
          }

          if (lNextIndex == fNextIndex) {
            lCurrent = j + 1;
            break;
          }
        }
      } else {
        for (int iR = f[i][0]; (f[i][0] == fNextIndex) ? iR < fNextIndex + 1 : iR <= fNextIndex; iR++) {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {

            int valueFirst = f[i][1];
            int valueSecond = this.getHistogramValue(iR, l, timestamps);

            setMapValue(map, valueFirst, valueSecond, 1);
          }
        }
      }
    }
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

  private void setMapValue(Map<Integer, Map<Integer, Integer>> map, int valueFirst, int valueSecond,
      int sum) {
    if (map.get(valueFirst) == null) {
      map.put(valueFirst, new HashMap<>());
      map.get(valueFirst).putIfAbsent(valueSecond, sum);
    } else {
      if (map.get(valueFirst).get(valueSecond) == null) {
        map.get(valueFirst).putIfAbsent(valueSecond, sum);
      } else {
        map.get(valueFirst).computeIfPresent(valueSecond, (k, v) -> v + sum);
      }
    }
  }

}
