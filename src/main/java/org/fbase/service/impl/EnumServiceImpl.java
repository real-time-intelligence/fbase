package org.fbase.service.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.Converter;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.MetaModel;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.EnumService;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.column.EColumn;
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
  public List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException {
    byte tableId = getTableId(tableName, metaModel);
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    if (!getTimestampProfile(cProfiles).getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Timestamp column not defined..");
    }

    if (cProfile.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Not supported for timestamp column..");
    }

    List<StackedColumn> list = new ArrayList<>();

    long previousBlockId = this.rawDAO.getPreviousBlockId(tableId, begin);

    if (previousBlockId != begin & previousBlockId != 0) {
      this.computeNoIndexBeginEnd(tableName, cProfile, previousBlockId, begin, end, list);
    }

    for (Long blockId : this.rawDAO.getListBlockIds(tableId, begin, end)) {
      this.computeNoIndexBeginEnd(tableName, cProfile, blockId, begin, end, list);
    }

    return list;
  }

  private void computeNoIndexBeginEnd(String tableName, CProfile cProfile,
      long blockId, long begin, long end, List<StackedColumn> list) {
    byte tableId = getTableId(tableName, metaModel);
    List<CProfile> cProfiles = getCProfiles(tableName, metaModel);

    CProfile tsProfile = getTimestampProfile(cProfiles);

    Map<Byte, Integer> map = new LinkedHashMap<>();

    long[] timestamps = this.rawDAO.getRawLong(tableId, blockId, tsProfile.getColId());

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
