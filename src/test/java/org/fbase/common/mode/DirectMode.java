package org.fbase.common.mode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.metadata.DataType;
import org.fbase.model.FBaseTestConfig;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

public interface DirectMode {

  default SProfile getSProfileDirect(
      String tableName,
      TType tableType,
      IType indexType,
      BType backendType,
      boolean compression
  ) {
    SProfile sProfile = SProfile.builder()
        .tableName(tableName)
        .tableType(tableType)
        .indexType(indexType)
        .backendType(backendType)
        .compression(compression)
        .csTypeMap(new LinkedHashMap<>()).build();

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("LONG_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("DOUBLE_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.DOUBLE).dType(DataType.DOUBLE).build());
    csTypeMap.put("STRING_FIELD", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.STRING).build());

    sProfile.setCsTypeMap(csTypeMap);

    return sProfile;
  }

  default void loadMetadataDirect(
      FBaseTestConfig config,
      String tableName,
      TType tableType,
      IType indexType,
      BType backendType,
      boolean compression
  ) throws TableNameEmptyException {
    SProfile sProfile = getSProfileDirect(tableName, tableType, indexType, backendType, compression);
    config.getFStore().loadDirectTableMetadata(sProfile);
  }

  default TProfile setupDirectProfile(String tableName, FBaseTestConfig configDirect) throws TableNameEmptyException {
    SProfile sProfile = getSProfileDirect(tableName, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, true);
    return configDirect.getFStore().loadDirectTableMetadata(sProfile);
  }

  default List<List<Object>> prepareData() {
    return Arrays.asList(
        addValue(1707387748310L),
        addValue(17073877482L),
        addValue(17073877482D),
        addValue("a31bce67-d1ab-485b-9ffa-850385e298ac")
    );
  }

  default <T> ArrayList<T> addValue(T value) {
    ArrayList<T> list = new ArrayList<>(1);
    list.add(value);
    return list;
  }
}
