package org.fbase.common.mode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.fbase.core.FStore;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

public interface JDBCMode {

  default SProfile getSProfileJdbc(
      String tableName,
      TType tableType,
      IType indexType,
      BType backendType,
      boolean compression
  ) {
    return SProfile.builder()
        .tableName(tableName)
        .tableType(tableType)
        .indexType(indexType)
        .backendType(backendType)
        .compression(compression)
        .csTypeMap(new LinkedHashMap<>()).build();
  }

  default void putData(FStore fStore, TProfile tProfile, Connection connection, String select, boolean isBatch) throws SQLException {

    try (PreparedStatement ps = connection.prepareStatement(select, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY); ResultSet r = ps.executeQuery()) {

      if (isBatch) {
        fStore.putDataJdbcBatch(tProfile.getTableName(), r, 10);
      } else {
        fStore.putDataJdbc(tProfile.getTableName(), r);
      }
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }
  }

  default void putDataJdbc(FStore fStore, TProfile tProfile, Connection connection, String select) throws SQLException {
    putData(fStore, tProfile, connection, select, false);
  }

  default void putDataJdbcBatch(FStore fStore, TProfile tProfile, Connection connection, String select) throws SQLException {
    putData(fStore, tProfile, connection, select, true);
  }
}

