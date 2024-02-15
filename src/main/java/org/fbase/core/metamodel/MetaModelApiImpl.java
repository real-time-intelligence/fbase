package org.fbase.core.metamodel;

import java.util.List;
import java.util.Objects;
import org.fbase.model.MetaModel;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

public class MetaModelApiImpl implements MetaModelApi {

  private final MetaModel metaModel;

  public MetaModelApiImpl(MetaModel metaModel) {
    this.metaModel = metaModel;
  }

  @Override
  public byte getTableId(String tableName) {
    return metaModel.getMetadata().get(tableName).getTableId();
  }

  @Override
  public String getTableName(Byte tableId) {
    return metaModel.getMetadata().entrySet().stream()
        .filter(f -> Objects.equals(f.getValue().getTableId(), tableId))
        .findAny()
        .orElseThrow()
        .getKey();
  }

  @Override
  public TType getTableType(String tableName) {
    return metaModel.getMetadata().get(tableName).getTableType();
  }

  @Override
  public IType getIndexType(String tableName) {
    return metaModel.getMetadata().get(tableName).getIndexType();
  }

  @Override
  public BType getBackendType(String tableName) {
    return metaModel.getMetadata().get(tableName).getBackendType();
  }

  @Override
  public Boolean getTableCompression(String tableName) {
    return metaModel.getMetadata().get(tableName).getCompression();
  }

  @Override
  public List<CProfile> getCProfiles(String tableName) {
    return metaModel.getMetadata().get(tableName).getCProfiles();
  }

  @Override
  public List<CProfile> getCProfiles(Byte tableId) {
   return metaModel.getMetadata().entrySet().stream()
        .filter(f -> Objects.equals(f.getValue().getTableId(), tableId))
        .findAny()
        .orElseThrow()
        .getValue()
        .getCProfiles();
  }

  @Override
  public CProfile getTimestampCProfile(String tableName) {
    return metaModel.getMetadata().get(tableName).getCProfiles().stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny()
        .orElseThrow(() -> new RuntimeException("Not found timestamp column"));
  }
}
