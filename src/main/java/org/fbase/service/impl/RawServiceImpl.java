package org.fbase.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.Converter;
import org.fbase.core.Mapper;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.output.StackedColumn;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.RawService;
import org.fbase.service.container.RawContainer;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.dto.MetadataDto;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class RawServiceImpl extends CommonServiceApi implements RawService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final MetadataDAO metadataDAO;
  private final HistogramDAO histogramDAO;
  private final EnumDAO enumDAO;

  public RawServiceImpl(MetaModel metaModel, Converter converter,
      RawDAO rawDAO, MetadataDAO metadataDAO, HistogramDAO histogramDAO, EnumDAO enumDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.metadataDAO = metadataDAO;
    this.histogramDAO = histogramDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException {
    byte tableId = getTableId(tProfile, metaModel);
    CProfile tsProfile = getTimestampProfile(getCProfiles(tProfile, metaModel));

    if (!tsProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long prevKey = this.rawDAO.getPreviousKey(tableId, begin);

    if (prevKey != begin & prevKey != 0) {
      this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, prevKey, begin, end, list);
    }

    for (MetadataDto e : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeNoIndexBeginEnd(tableId, tsProfile, cProfile, e.getKey(), begin, end, list);
    }

    return list;
  }

  @Override
  public List<List<Object>> getRawDataAll(TProfile tProfile, long begin, long end) {
    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);
    return getRawData(tProfile, cProfiles, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(TProfile tProfile, CProfile cProfile, long begin, long end) {
    CProfile tsProfile = getTsProfile(tProfile);
    List<CProfile> cProfiles = List.of(tsProfile, cProfile);
    return getRawData(tProfile, cProfiles, begin, end);
  }

  private List<List<Object>> getRawData(TProfile tProfile, List<CProfile> cProfiles, long begin, long end) {
    byte tableId = getTableId(tProfile, metaModel);
    CProfile tsProfile = getTsProfile(tProfile);

    List<List<Object>> columnDataListOut = new ArrayList<>();

    long prevKey = this.rawDAO.getPreviousKey(tableId, begin);
    if (prevKey != begin & prevKey != 0) {
      MetadataDto mDto = this.metadataDAO.getMetadata(tableId, prevKey);
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, mDto, begin, end, columnDataListOut);
    }

    for (MetadataDto mDto : this.metadataDAO.getListMetadata(tableId, begin, end)) {
      this.computeRawDataBeginEnd(tableId, tsProfile, cProfiles, mDto, begin, end, columnDataListOut);
    }

    return columnDataListOut;
  }

  private CProfile getTsProfile(TProfile tProfile) {
    return getCProfiles(tProfile, metaModel).stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findFirst()
        .orElseThrow();
  }

  private void computeRawDataBeginEnd(byte tableId, CProfile tsProfile, List<CProfile> cProfiles, MetadataDto mdto,
      long begin, long end, List<List<Object>> columnDataListOut) {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    long[] timestamps = rawDAO.getRawLong(tableId, mdto.getKey(), tsProfile.getColId());

    cProfiles.forEach(cProfile -> {

      if (cProfile.getCsType().isTimeStamp()) { // timestamp
        List<Object> outVar = new ArrayList<>();

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            outVar.add(timestamps[i]);
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 0 : cProfile.getColId(), outVar);
      }

      List<Object> outVar = new ArrayList<>();

      if (cProfile.getCsType().getSType() == SType.RAW & !cProfile.getCsType().isTimeStamp()) { // raw data

        RawContainer rawContainer =
            new RawContainer(mdto.getKey(), cProfile, this.rawDAO.getRawData(tableId, mdto.getKey(), cProfile.getColId()));

        IntStream iRow = IntStream.range(0, timestamps.length);
        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            outVar.add(rawContainer.getStrValueForCell(iR));
          }
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), outVar);
      }

      if (cProfile.getCsType().getSType() == SType.HISTOGRAM) { // indexed data
        int[][] h = histogramDAO.get(mdto.getHistograms()[cProfile.getColId()]);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] >= begin & timestamps[i] <= end) {
            outVar.add(this.converter.convertIntToRaw(getHistogramValue(i, h, timestamps), cProfile));
          }
        }

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), outVar);
      }

      if (cProfile.getCsType().getSType() == SType.ENUM) { // enum data

        RawContainer rawContainer =
            new RawContainer(mdto.getKey(), cProfile, this.rawDAO.getRawData(tableId, mdto.getKey(), cProfile.getColId()));

        IntStream iRow = IntStream.range(0, timestamps.length);

        int[] eColumn = enumDAO.getEColumnValues(tableId, cProfile.getColId());

        iRow.forEach(iR -> {
          if (timestamps[iR] >= begin & timestamps[iR] <= end) {
            byte var = rawContainer.getEnumValueForCell(iR);
            outVar.add(converter.convertIntToRaw(EnumHelper.getIndexValue(eColumn, var), cProfile));}
        });

        columnDataListLocal.add(cProfiles.size() == 2 ? 1 : cProfile.getColId(), outVar);
      }

    });

    columnDataListOut.addAll(transpose(columnDataListLocal));
  }

  private void computeNoIndexBeginEnd(byte tableId, CProfile tProfile, CProfile cProfile,
      long key, long begin, long end, List<StackedColumn> list) {

    Map<String, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, key, tProfile.getColId());

    RawContainer rawContainer = new RawContainer(key, cProfile,
        this.rawDAO.getRawData(tableId, key, cProfile.getColId()));

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        map.compute(rawContainer.getStrValueForCell(iR), (k, val) -> val == null ? 1 : val + 1);
      }
    });

    list.add(StackedColumn.builder()
        .key(key)
        .tail(tail)
        .keyCount(map).build());
  }

  public static <T> List<List<T>> transpose(List<List<T>> table) {
    List<List<T>> ret = new ArrayList<List<T>>();
    final int N = table.stream().mapToInt(List::size).max().orElse(-1);
    Iterator[] iters = new Iterator[table.size()];

    int i=0;
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
