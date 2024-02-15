package org.fbase.core.metamodel;

import java.util.List;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

public interface MetaModelApi {

  byte getTableId(String tableName);
  String getTableName(Byte tableId);

  TType getTableType(String tableName);

  IType getIndexType(String tableName);

  BType getBackendType(String tableName);

  Boolean getTableCompression(String tableName);

  List<CProfile> getCProfiles(String tableName);

  List<CProfile> getCProfiles(Byte tableId);

  CProfile getTimestampProfile(String tableName);
}
