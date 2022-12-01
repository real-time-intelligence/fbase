package org.fbase.service.impl;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.Converter;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.MetaModel;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.EnumService;
import org.fbase.service.container.RawContainer;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.dto.MetadataDto;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class EnumServiceImpl extends CommonServiceApi implements EnumService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final RawDAO rawDAO;
  private final EnumDAO enumDAO;

  public EnumServiceImpl(MetaModel metaModel, Converter converter, RawDAO rawDAO, EnumDAO enumDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.rawDAO = rawDAO;
    this.enumDAO = enumDAO;
  }

  @Override
  public List<StackedColumn> getListStackedColumn(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException {
    byte tableId = getTableId(tProfile, metaModel);
    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);

    if (!getTimestampProfile(cProfiles).getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long prevKey = this.rawDAO.getPreviousKey(tableId, begin);

    if (prevKey != begin & prevKey != 0) {
      this.computeNoIndexBeginEnd(tProfile, cProfile, prevKey, begin, end, list);
    }

    for (Long e : this.rawDAO.getListKeys(tableId, begin, end)) {
      this.computeNoIndexBeginEnd(tProfile, cProfile, e, begin, end, list);
    }

    return list;
  }

  private void computeNoIndexBeginEnd(TProfile tProfile, CProfile cProfile,
      long key, long begin, long end, List<StackedColumn> list) {
    byte tableId = getTableId(tProfile, metaModel);
    List<CProfile> cProfiles = getCProfiles(tProfile, metaModel);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    Map<Byte, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, key, tsProfile.getColId());

    RawContainer rawContainer = new RawContainer(key, cProfile,
        this.rawDAO.getRawData(tableId, key, cProfile.getColId()));

    long tail = timestamps[timestamps.length - 1];

    IntStream iRow = IntStream.range(0, timestamps.length);
    iRow.forEach(iR -> {
      if (timestamps[iR] >= begin & timestamps[iR] <= end) {
        byte var = rawContainer.getEnumValueForCell(iR);
        map.compute(var, (k, val) -> val == null ? 1 : val + 1);
      }
    });

    Map<String, Integer> mapKeyCount = new LinkedHashMap<>();
    int[] eColumn = enumDAO.getEColumnValues(tableId, cProfile.getColId());

    map.forEach((keyByte, value) -> mapKeyCount.put(converter.convertIntToRaw(
        EnumHelper.getIndexValue(eColumn, keyByte), cProfile), value));

    list.add(StackedColumn.builder()
        .key(key)
        .tail(tail)
        .keyCount(mapKeyCount).build());
  }

}
