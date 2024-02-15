package org.fbase.service.impl;

import static org.fbase.metadata.DataType.ARRAY;
import static org.fbase.metadata.DataType.MAP;

import com.sleepycat.persist.EntityCursor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByService;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class GroupByServiceImpl extends CommonServiceApi implements GroupByService {
  private final MetaModelApi metaModelApi;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByServiceImpl(MetaModelApi metaModelApi,
                            Converter converter,
                            HistogramDAO histogramDAO,
                            RawDAO rawDAO,
                            EnumDAO enumDAO) {
    this.metaModelApi = metaModelApi;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<GanttColumn> getListGanttColumn(String tableName,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              long begin,
                                              long end) {
    TType tableType = metaModelApi.getTableType(tableName);

    if (IType.GLOBAL.equals(tableType)) {
      return getListGanttColumnIndexGlobal(tableName, firstGrpBy, secondGrpBy, begin, end);
    } else if (IType.LOCAL.equals(tableType)) {
      return getListGanttColumnIndexLocal(tableName, firstGrpBy, secondGrpBy, begin, end);
    } else {
      return getListGanttColumnIndexLocal(tableName, firstGrpBy, secondGrpBy, begin, end);
    }
  }

  private List<GanttColumn> getListGanttColumnIndexLocal(String tableName,
                                                         CProfile firstGrpBy,
                                                         CProfile secondGrpBy,
                                                         long begin,
                                                         long end) {

    byte tableId = metaModelApi.getTableId(tableName);

    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    int firstColId = firstGrpBy.getColId();
    int secondColId = secondGrpBy.getColId();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    Map.Entry<MetadataKey, MetadataKey> keyEntry = getMetadataKeyPair(tableId, begin, end, previousBlockId);

    Map<String, Map<String, Integer>> mapFinal = new HashMap<>();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(keyEntry.getKey(), keyEntry.getValue())) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        SType firstSType = getSType(firstColId, columnKey);
        SType secondSType = getSType(secondColId, columnKey);

        if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.HISTOGRAM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeHistHist(tableId, blockId, firstGrpBy, secondGrpBy, tsColId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.ENUM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeEnumEnum(tableId, tsColId, blockId, firstGrpBy, secondGrpBy, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.RAW)) {
          Map<String, Map<String, Integer>> map = new HashMap<>();
          this.computeRawRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal, key, kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.ENUM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeHistEnum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.HISTOGRAM)) {
          Map<Integer, Map<Integer, Integer>> map = new HashMap<>();
          this.computeEnumHist(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy),
                                                                                converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.HISTOGRAM, SType.RAW)) {
          Map<Integer, Map<String, Integer>> map = new HashMap<>();
          this.computeHistRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy), kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.HISTOGRAM)) {
          Map<String, Map<Integer, Integer>> map = new HashMap<>();
          this.computeRawHist(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                key, converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.ENUM, SType.RAW)) {
          Map<Integer, Map<String, Integer>> map = new HashMap<>();
          this.computeEnumRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                converter.convertIntToRaw(key, firstGrpBy), kVal, vVal)));

        } else if (checkSTypeILocal(firstSType, secondSType, SType.RAW, SType.ENUM)) {
          Map<String, Map<Integer, Integer>> map = new HashMap<>();
          this.computeRawEnum(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map);

          map.forEach((key, value) -> value.forEach((kVal, vVal) -> setMapValue(mapFinal,
                                                                                key, converter.convertIntToRaw(kVal, secondGrpBy), vVal)));

        }
      }

    } catch (Exception e) {
      log.error(e.getMessage());
    }

    if (ARRAY.equals(firstGrpBy.getCsType().getDType()) || ARRAY.equals(secondGrpBy.getCsType().getDType())) {
      Map<String, Map<String, Integer>> map = handleArray(firstGrpBy, secondGrpBy, mapFinal);
      mapFinal.clear();
      mapFinal.putAll(map);
    }

    if (MAP.equals(firstGrpBy.getCsType().getDType()) || MAP.equals(secondGrpBy.getCsType().getDType())) {
      return handleMap(firstGrpBy, secondGrpBy, mapFinal);
    }

    List<GanttColumn> list = new ArrayList<>();

    mapFinal.forEach((key, value) -> list.add(GanttColumn.builder().key(key).gantt(value).build()));

    return list;
  }

  private List<GanttColumn> getListGanttColumnIndexGlobal(String tableName,
                                                          CProfile firstGrpBy,
                                                          CProfile secondGrpBy,
                                                          long begin,
                                                          long end) {

    byte tableId = metaModelApi.getTableId(tableName);

    int tsColId = metaModelApi.getTimestampCProfile(tableName).getColId();

    List<GanttColumn> list = new ArrayList<>();

    if (checkSType(firstGrpBy, secondGrpBy, SType.HISTOGRAM, SType.HISTOGRAM)) {
      histHist(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.ENUM, SType.ENUM)) {
      enumEnum(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.RAW, SType.RAW)) {
      rawRaw(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.HISTOGRAM, SType.ENUM)) {
      histEnum(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.ENUM, SType.HISTOGRAM)) {
      enumHist(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.HISTOGRAM, SType.RAW)) {
      histRaw(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.RAW, SType.HISTOGRAM)) {
      rawHist(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.ENUM, SType.RAW)) {
      enumRaw(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    } else if (checkSType(firstGrpBy, secondGrpBy, SType.RAW, SType.ENUM)) {
      rawEnum(tableId, tsColId, firstGrpBy, secondGrpBy, begin, end, list);
    }

    return list;
  }

  private void enumEnum(byte tableId,
                        int tsColId,
                        CProfile firstGrpBy,
                        CProfile secondGrpBy,
                        long begin,
                        long end,
                        List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeEnumEnum(tableId, tsColId, previousBlockId, firstGrpBy, secondGrpBy,
                           begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeEnumEnum(tableId, tsColId, blockId,
                                                 firstGrpBy, secondGrpBy, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
                                             .key(this.converter.convertIntToRaw(key, firstGrpBy))
                                             .gantt(getEnumBlockMap(value, secondGrpBy)).build()));
  }

  private void computeEnumEnum(byte tableId,
                               int tsColId,
                               long blockId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               long begin,
                               long end,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map.Entry<int[], byte[]> listFirst = computeEnumEnum(tableId, firstGrpBy, timestamp, blockId, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumEnum(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueEnumEnumBlock(map, listFirst, listSecond, 1);
  }

  private void rawRaw(byte tableId,
                      int tsColId,
                      CProfile firstGrpBy,
                      CProfile secondGrpBy,
                      long begin,
                      long end,
                      List<GanttColumn> list) {

    Map<String, Map<String, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawRaw(tableId, firstGrpBy, secondGrpBy, tsColId,
                         previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId ->
                     this.computeRawRaw(tableId, firstGrpBy, secondGrpBy, tsColId, blockId, begin, end, map)
        );

    map.forEach((key, value) -> list.add(GanttColumn.builder().key(key).gantt(value).build()));
  }

  private void computeRawRaw(byte tableId,
                             CProfile firstGrpBy,
                             CProfile secondGrpBy,
                             int tsColId,
                             long blockId,
                             long begin,
                             long end,
                             Map<String, Map<String, Integer>> map) {

    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

    String[] first = getStringArrayValue(rawDAO, tableId, blockId, firstGrpBy);
    String[] second = getStringArrayValue(rawDAO, tableId, blockId, secondGrpBy);

    if (first.length != 0 & second.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          setMapValue(map, first[iR], second[iR], 1);
        }
      });
    }
  }

  private void histEnum(byte tableId,
                        int tsColId,
                        CProfile firstGrpBy,
                        CProfile secondGrpBy,
                        long begin,
                        long end,
                        List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeHistEnum(tableId, firstGrpBy, secondGrpBy,
                           tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeHistEnum(tableId, firstGrpBy, secondGrpBy,
                                                 tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
                                             .key(this.converter.convertIntToRaw(key, firstGrpBy))
                                             .gantt(getEnumBlockMap(value, secondGrpBy)).build()));
  }

  private void computeHistEnum(byte tableId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long blockId,
                               long begin,
                               long end,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    List<Integer> listFirst = computeHistogram(tableId, firstGrpBy, timestamp, blockId, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumBlock(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueHistEnumBlock(map, listFirst, listSecond, 1);
  }

  private List<Integer> computeHistogram(byte tableId,
                                         CProfile cProfile,
                                         long[] timestamp,
                                         long blockId,
                                         long begin,
                                         long end) {

    List<Integer> list = new ArrayList<>();

    int[][] h = histogramDAO.get(tableId, blockId, cProfile.getColId());

    for (int i = 0; i < h[0].length; i++) {
      int fNextIndex = getNextIndex(i, h, timestamp);
      int startIndex;

      if (i == 0) {
        startIndex = 0;
      } else {
        startIndex = fNextIndex - (fNextIndex - getNextIndex(i - 1, h, timestamp)) + 1;
      }

      for (int k = startIndex; k <= fNextIndex; k++) {
        boolean checkRange = timestamp[k] >= begin & timestamp[k] <= end;
        if (checkRange) {
          list.add(h[1][i]);
        }
      }
    }

    return list;
  }

  private Map.Entry<int[], byte[]> computeEnumBlock(byte tableId,
                                                    CProfile cProfile,
                                                    long[] timestamp,
                                                    long blockId,
                                                    long begin,
                                                    long end) {

    byte[] eBytes = new byte[timestamp.length];

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    IntStream iRow = IntStream.range(0, timestamp.length);

    iRow.forEach(iR -> {
      if (timestamp[iR] >= begin & timestamp[iR] <= end) {
        eBytes[iR] = eColumn.getDataByte()[iR];
      }
    });

    return Map.entry(eColumn.getValues(), eBytes);
  }

  private Map.Entry<int[], byte[]> computeEnumEnum(byte tableId,
                                                   CProfile cProfile,
                                                   long[] timestamp,
                                                   long blockId,
                                                   long begin,
                                                   long end) {

    List<Byte> eBytes = new ArrayList<>();

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    IntStream iRow = IntStream.range(0, timestamp.length);

    iRow.forEach(iR -> {
      if (timestamp[iR] >= begin & timestamp[iR] <= end) {
        eBytes.add(eColumn.getDataByte()[iR]);
      }
    });

    return Map.entry(eColumn.getValues(), getByteFromList(eBytes));
  }

  private List<String> computeRaw(byte tableId,
                                  CProfile cProfile,
                                  long[] timestamp,
                                  long blockId,
                                  long begin,
                                  long end) {

    List<String> list = new ArrayList<>();

    String[] columnData = getStringArrayValue(rawDAO, tableId, blockId, cProfile);

    if (columnData.length != 0) {
      IntStream iRow = IntStream.range(0, timestamp.length);
      iRow.forEach(iR -> {
        if (timestamp[iR] >= begin & timestamp[iR] <= end) {
          list.add(columnData[iR]);
        }
      });
    }

    return list;
  }

  private void enumHist(byte tableId,
                        int tsColId,
                        CProfile firstGrpBy,
                        CProfile secondGrpBy,
                        long begin,
                        long end,
                        List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeEnumHist(tableId, firstGrpBy, secondGrpBy,
                           tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeEnumHist(tableId, firstGrpBy, secondGrpBy,
                                                 tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
                                             .key(this.converter.convertIntToRaw(key, firstGrpBy))
                                             .gantt(getHistogramGanttMap(value, secondGrpBy)).build()));
  }

  private void computeEnumHist(byte tableId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long blockId,
                               long begin,
                               long end,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map.Entry<int[], byte[]> listFirst = computeEnumEnum(tableId, firstGrpBy, timestamp, blockId, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueCommonBlockLevel(map, listFirst, listSecond, 1);
  }

  private void histHist(byte tableId,
                        int tsColId,
                        CProfile firstGrpBy,
                        CProfile secondGrpBy,
                        long begin,
                        long end,
                        List<GanttColumn> list) {

    Map<Integer, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);

    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeHistHist(tableId, previousBlockId, firstGrpBy, secondGrpBy, tsColId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> {
          this.computeHistHist(tableId, blockId, firstGrpBy, secondGrpBy, tsColId, begin, end, map);
        });

    this.convertMapToDto(firstGrpBy, secondGrpBy, map, list);
  }

  private void convertMapToDto(CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               Map<Integer, Map<Integer, Integer>> mapSource,
                               List<GanttColumn> listDest) {
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

  private void computeHistHist(byte tableId,
                               long blockId,
                               CProfile firstGrpBy,
                               CProfile secondGrpBy,
                               int tsColId,
                               long begin,
                               long end,
                               Map<Integer, Map<Integer, Integer>> map) {
    long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsColId);

    int[][] f = histogramDAO.get(tableId, blockId, firstGrpBy.getColId());
    int[][] l = histogramDAO.get(tableId, blockId, secondGrpBy.getColId());

    boolean checkRange = timestamps[f[0][0]] >= begin & timestamps[f[0][f[0].length - 1]] <= end;

    int lCurrent = 0;

    for (int i = 0; i < f[0].length; i++) {
      int fNextIndex = getNextIndex(i, f, timestamps);

      if (checkRange) {
        for (int j = lCurrent; j < l[0].length; j++) {
          int lNextIndex = getNextIndex(j, l, timestamps);

          if (lNextIndex <= fNextIndex) {
            if (l[0][j] <= f[0][i]) {
              setMapValue(map, f[1][i], l[1][j], (lNextIndex - f[0][i]) + 1);
            } else {
              setMapValue(map, f[1][i], l[1][j], (lNextIndex - l[0][j]) + 1);
            }
          } else {
            if (f[0][i] <= l[0][j]) {
              setMapValue(map, f[1][i], l[1][j], (fNextIndex - l[0][j]) + 1);
            } else {
              setMapValue(map, f[1][i], l[1][j], (fNextIndex - f[0][i]) + 1);
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
        for (int iR = f[0][i]; (f[0][i] == fNextIndex) ? iR < fNextIndex + 1 : iR <= fNextIndex; iR++) {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {

            int valueFirst = f[1][i];
            int valueSecond = getHistogramValue(iR, l, timestamps);

            setMapValue(map, valueFirst, valueSecond, 1);
          }
        }
      }
    }
  }

  private void histRaw(byte tableId,
                       int tsColId,
                       CProfile firstGrpBy,
                       CProfile secondGrpBy,
                       long begin,
                       long end,
                       List<GanttColumn> list) {

    Map<Integer, Map<String, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeHistRaw(tableId, firstGrpBy, secondGrpBy,
                          tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeHistRaw(tableId, firstGrpBy, secondGrpBy,
                                                tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> {
      String keyStr = this.converter.convertIntToRaw(key, firstGrpBy);
      list.add(GanttColumn.builder().key(keyStr).gantt(value).build());
    });
  }

  private void computeHistRaw(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              Map<Integer, Map<String, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    List<Integer> listFirst = computeHistogram(tableId, firstGrpBy, timestamp, blockId, begin, end);
    List<String> listSecond = computeRaw(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  private void rawHist(byte tableId,
                       int tsColId,
                       CProfile firstGrpBy,
                       CProfile secondGrpBy,
                       long begin,
                       long end,
                       List<GanttColumn> list) {

    Map<String, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawHist(tableId, firstGrpBy, secondGrpBy,
                          tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeRawHist(tableId, firstGrpBy, secondGrpBy,
                                                tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> {
      list.add(GanttColumn.builder().key(key).gantt(getHistogramGanttMap(value, secondGrpBy)).build());
    });
  }

  private void computeRawHist(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              Map<String, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    List<String> listFirst = computeRaw(tableId, firstGrpBy, timestamp, blockId, begin, end);
    List<Integer> listSecond = computeHistogram(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueCommon(map, listFirst, listSecond, 1);
  }

  private void enumRaw(byte tableId,
                       int tsColId,
                       CProfile firstGrpBy,
                       CProfile secondGrpBy,
                       long begin,
                       long end,
                       List<GanttColumn> list) {

    Map<Integer, Map<String, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeEnumRaw(tableId, firstGrpBy, secondGrpBy,
                          tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeEnumRaw(tableId, firstGrpBy, secondGrpBy,
                                                tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
                                             .key(this.converter.convertIntToRaw(key, firstGrpBy))
                                             .gantt(value).build()));
  }

  private void computeEnumRaw(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              Map<Integer, Map<String, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    Map.Entry<int[], byte[]> listFirst = computeEnumEnum(tableId, firstGrpBy, timestamp, blockId, begin, end);
    List<String> listSecond = computeRaw(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueEnumRawBlock(map, listFirst, listSecond, 1);
  }

  private void rawEnum(byte tableId,
                       int tsColId,
                       CProfile firstGrpBy,
                       CProfile secondGrpBy,
                       long begin,
                       long end,
                       List<GanttColumn> list) {

    Map<String, Map<Integer, Integer>> map = new HashMap<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeRawEnum(tableId, firstGrpBy, secondGrpBy,
                          tsColId, previousBlockId, begin, end, map);
    }

    this.rawDAO.getListBlockIds(tableId, begin, end)
        .forEach(blockId -> this.computeRawEnum(tableId, firstGrpBy, secondGrpBy,
                                                tsColId, blockId, begin, end, map));

    map.forEach((key, value) -> list.add(GanttColumn.builder()
                                             .key(key)
                                             .gantt(getEnumBlockMap(value, secondGrpBy)).build()));
  }

  private void computeRawEnum(byte tableId,
                              CProfile firstGrpBy,
                              CProfile secondGrpBy,
                              int tsColId,
                              long blockId,
                              long begin,
                              long end,
                              Map<String, Map<Integer, Integer>> map) {
    long[] timestamp = rawDAO.getRawLong(tableId, blockId, tsColId);

    List<String> listFirst = computeRaw(tableId, firstGrpBy, timestamp, blockId, begin, end);
    Map.Entry<int[], byte[]> listSecond = computeEnumBlock(tableId, secondGrpBy, timestamp, blockId, begin, end);

    setMapValueRawEnumBlock(map, listFirst, listSecond, 1);
  }

  private boolean checkSType(CProfile firstGrpBy,
                             CProfile secondGrpBy,
                             SType firstSType,
                             SType secondSType) {
    return firstGrpBy.getCsType().getSType().equals(firstSType) &
        secondGrpBy.getCsType().getSType().equals(secondSType);
  }

  private boolean checkSTypeILocal(SType first,
                                   SType second,
                                   SType firstCompare,
                                   SType secondCompare) {
    return first.equals(firstCompare) & second.equals(secondCompare);
  }

  private <T, V> void setMapValueCommon(Map<T, Map<V, Integer>> map,
                                        List<T> listFirst,
                                        List<V> listSecond,
                                        int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      setMapValue(map, listFirst.get(i), listSecond.get(i), sum);
    }
  }

  private void setMapValueEnumEnumBlock(Map<Integer, Map<Integer, Integer>> map,
                                        Map.Entry<int[], byte[]> entryFirst,
                                        Map.Entry<int[], byte[]> entrySecond,
                                        int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      int intToRawSecond = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueEnumBlock(map, intToRawFirst, intToRawSecond, sum);
    }
  }

  private void setMapValueHistEnumBlock(Map<Integer, Map<Integer, Integer>> map,
                                        List<Integer> listFirst,
                                        Map.Entry<int[], byte[]> entrySecond,
                                        int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      int intToRaw = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueEnumBlock(map, listFirst.get(i), intToRaw, sum);
    }
  }

  private void setMapValueRawEnumBlock(Map<String, Map<Integer, Integer>> map,
                                       List<String> listFirst,
                                       Map.Entry<int[], byte[]> entrySecond,
                                       int sum) {
    for (int i = 0; i < listFirst.size(); i++) {
      int intToRaw = EnumHelper.getIndexValue(entrySecond.getKey(), entrySecond.getValue()[i]);
      setMapValueRawEnumBlock(map, listFirst.get(i), intToRaw, sum);
    }
  }

  private void setMapValueCommonBlockLevel(Map<Integer, Map<Integer, Integer>> map,
                                           Map.Entry<int[], byte[]> entryFirst,
                                           List<Integer> listSecond,
                                           int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      setMapValueEnumBlock(map, intToRawFirst, listSecond.get(i), sum);
    }
  }

  private void setMapValueEnumRawBlock(Map<Integer, Map<String, Integer>> map,
                                       Map.Entry<int[], byte[]> entryFirst,
                                       List<String> listSecond,
                                       int sum) {
    for (int i = 0; i < entryFirst.getValue().length; i++) {
      int intToRawFirst = EnumHelper.getIndexValue(entryFirst.getKey(), entryFirst.getValue()[i]);
      setMapValueRawEnumBlock(map, intToRawFirst, listSecond.get(i), sum);
    }
  }

  private Map<String, Integer> getHistogramGanttMap(Map<Integer, Integer> value,
                                                    CProfile secondGrpBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> this.converter.convertIntToRaw(k.getKey(), secondGrpBy),
                                  Map.Entry::getValue));
  }

  private Map<String, Integer> getEnumBlockMap(Map<Integer, Integer> value,
                                               CProfile secondGrpBy) {
    return value.entrySet()
        .stream()
        .collect(Collectors.toMap(k -> converter.convertIntToRaw(k.getKey(), secondGrpBy), Map.Entry::getValue));
  }
}