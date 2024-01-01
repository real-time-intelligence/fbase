package org.fbase.service.impl;

import com.sleepycat.persist.EntityCursor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.MetaModel;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.GroupByOneService;
import org.fbase.storage.Converter;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class GroupByOneServiceImpl extends CommonServiceApi implements GroupByOneService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public GroupByOneServiceImpl(MetaModel metaModel, Converter converter, HistogramDAO histogramDAO,
                               RawDAO rawDAO, EnumDAO enumDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end) throws SqlColMetadataException {
    CProfile tsProfile = getTimestampProfile(getCProfiles(tableName, metaModel));

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    return this.getListStackedColumn(tableName, tsProfile, cProfile, begin, end);
  }

  public List<StackedColumn> getListStackedColumn(String tableName, CProfile tsProfile,
                                                  CProfile cProfile, long begin, long end) {
    byte tableId = getTableId(tableName, metaModel);

    List<StackedColumn> list = new ArrayList<>();

    MetadataKey beginMKey;
    MetadataKey endMKey;

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);
    if (previousBlockId != begin & previousBlockId != 0) {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(previousBlockId).build();
    } else {
      beginMKey = MetadataKey.builder().tableId(tableId).blockId(begin).build();
    }
    endMKey = MetadataKey.builder().tableId(tableId).blockId(end).build();

    try (EntityCursor<Metadata> cursor = rawDAO.getMetadataEntityCursor(beginMKey, endMKey)) {
      Metadata columnKey;

      while ((columnKey = cursor.next()) != null) {
        long blockId = columnKey.getMetadataKey().getBlockId();

        long[] timestamps = rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

        if (tsProfile.getColId() == cProfile.getColId()) {
          this.computeRaw(tableId, cProfile, blockId, timestamps, begin, end, list);
        } else {
          SType sType = getSType(cProfile.getColId(), columnKey);

          if (SType.RAW.equals(sType)) {
            this.computeRaw(tableId, cProfile, blockId, timestamps, begin, end, list);
          }

          if (SType.HISTOGRAM.equals(sType)) {
            if (previousBlockId == blockId) {
              this.computeHistTailOverFlow(tableId, cProfile, blockId, timestamps, begin, end, list);
            } else {
              this.computeHistFull(tableId, cProfile, blockId, timestamps, list);
            }
          }

          if (SType.ENUM.equals(sType)) {
            this.computeEnum(tableId, cProfile, blockId, timestamps, begin, end, list);
          }
        }
      }
    } catch (Exception e) {
      log.catching(e);
      log.error(e.getMessage());
    }

    return list;
  }

  private void computeHistFull(byte tableId, CProfile cProfile, long blockId, long[] timestamps,
                               List<StackedColumn> list) {

    Map<Integer, Integer> map = new LinkedHashMap<>();

    long tail = timestamps[timestamps.length - 1];

    int[][] hData = histogramDAO.get(tableId, blockId, cProfile.getColId());

    IntStream iRow = IntStream.range(0, hData[0].length);
    iRow.forEach(iR -> {
      int deltaCountValue;

      if (iR == hData[0].length - 1) { //todo last row
        deltaCountValue = timestamps.length - hData[0][iR];
      } else {
        deltaCountValue = hData[0][iR + 1] - hData[0][iR];
      }

      map.compute(hData[1][iR], (k, val) -> val == null ? deltaCountValue : val + deltaCountValue);
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();
    map.forEach((keyInt, value) -> mapKeyCount
            .put(this.converter.convertIntToRaw(keyInt, cProfile), value));

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
          .key(blockId)
          .tail(tail)
          .keyCount(mapKeyCount).build());
    }
  }

  private void computeHistTailOverFlow(byte tableId, CProfile cProfile, long blockId,
                                       long[] timestamps, long begin, long end, List<StackedColumn> list) {

    long tail = timestamps[timestamps.length - 1];

    int[][] histograms = histogramDAO.get(tableId, blockId, cProfile.getColId());

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
          histogramsUnPack[i] = histograms[1][0];
        }
      }
    }

    Map<String, Integer> map = new LinkedHashMap<>();
    IntStream iRow = IntStream.range(0, timestamps.length);

    if (blockId < begin) {
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          int objectIndex;
          if (histogramsUnPack[iR] == 0) {
            objectIndex = histogramsUnPack[0];
          } else {
            objectIndex = histogramsUnPack[iR];
          }

          String keyCompute = this.converter.convertIntToRaw(objectIndex, cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (blockId >= begin & tail > end) {
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          String keyCompute = this.converter.convertIntToRaw(histogramsUnPack[iR], cProfile);
          map.compute(keyCompute, (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
          .key(blockId)
          .tail(tail)
          .keyCount(map).build());
    }
  }

  private void computeRaw(byte tableId, CProfile cProfile, long blockId,
                          long[] timestamps, long begin, long end, List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

    String[] column = getStringArrayValue(rawDAO, tableId, blockId, cProfile);

    long tail = timestamps[timestamps.length - 1];

    if (column.length != 0) {
      IntStream iRow = IntStream.range(0, timestamps.length);
      iRow.forEach(iR -> {
        if (timestamps[iR] >= begin & timestamps[iR] <= end) {
          map.compute(column[iR], (k, val) -> val == null ? 1 : val + 1);
        }
      });
    }

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
          .key(blockId)
          .tail(tail)
          .keyCount(map).build());
    }
  }

  private void computeEnum(byte tableId, CProfile cProfile, long blockId,
                          long[] timestamps, long begin, long end, List<StackedColumn> list) {
    Map<Byte, Integer> map = new LinkedHashMap<>();

    EColumn eColumn = enumDAO.getEColumnValues(tableId, blockId, cProfile.getColId());

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(eColumn.getDataByte()[iR], (k, val) -> val == null ? 1 : val + 1);
      }
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();

    map.forEach((keyByte, value) -> mapKeyCount.put(converter.convertIntToRaw(
            EnumHelper.getIndexValue(eColumn.getValues(), keyByte), cProfile), value));

    if (!map.isEmpty()) {
      list.add(StackedColumn.builder()
          .key(blockId)
          .tail(tail)
          .keyCount(mapKeyCount).build());
    }
  }
}