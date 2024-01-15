package org.fbase.service.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.CommonServiceApi;
import org.fbase.storage.helper.EnumHelper;
import org.fbase.util.CachedLastLinkedHashMap;

@Getter
@EqualsAndHashCode(callSuper = true)
public class EStore extends CommonServiceApi {

  private final int initialCapacity;

  private final List<List<Byte>> rawData;
  private final CachedLastLinkedHashMap<Integer, Integer> mapping;

  private final List<CachedLastLinkedHashMap<Integer, Byte>> rawDataEColumn;

  public EStore(List<CProfile> cProfiles,
                Map<Integer, SType> colIdSTypeMap) {
    this.initialCapacity = Math.toIntExact(cProfiles.stream()
                                               .filter(f -> !f.getCsType().isTimeStamp())
                                               .filter(f -> SType.ENUM.equals(colIdSTypeMap.get(f.getColId())))
                                               .count());

    this.rawData = new ArrayList<>(this.initialCapacity);
    this.mapping = new CachedLastLinkedHashMap<>();
    this.rawDataEColumn = new ArrayList<>(this.initialCapacity);

    fillArrayList(this.rawData, this.initialCapacity);
    fillAllEnumMappingSType(cProfiles, this.mapping, this.rawDataEColumn, colIdSTypeMap);
  }

  public void add(int iMapping,
                  int iR,
                  int curValue) throws EnumByteExceedException {
    this.rawData.get(iMapping).add(iR, EnumHelper.getByteValue(rawDataEColumn.get(iMapping), curValue));
  }
}
