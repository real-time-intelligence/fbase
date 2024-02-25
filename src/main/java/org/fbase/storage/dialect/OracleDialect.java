package org.fbase.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicReference;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;

public class OracleDialect implements DatabaseDialect {

  @Override
  public String getWhereClass(CProfile tsCProfile,
                              CProfile cProfileFilter,
                              String filter) {
    if (DataType.DATE.equals(tsCProfile.getCsType().getDType())) {
      return "WHERE " + tsCProfile.getColName().toLowerCase() + " BETWEEN ? AND ? " +
          getFilterAndString(cProfileFilter, filter);
    } else if (DataType.DATETIME.equals(tsCProfile.getCsType().getDType())) {
        return "WHERE " + tsCProfile.getColName().toLowerCase() + " BETWEEN ? AND ? " +
            getFilterAndString(cProfileFilter, filter);
    } else if (DataType.TIMESTAMP.equals(tsCProfile.getCsType().getDType())) {
      return "WHERE " + tsCProfile.getColName().toLowerCase() + " BETWEEN ? AND ? " +
          getFilterAndString(cProfileFilter, filter);
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  @Override
  public void setDateTime(CProfile tsCProfile,
                          PreparedStatement ps,
                          int parameterIndex,
                          long unixTimestamp) throws SQLException {

    if (DataType.DATE.equals(tsCProfile.getCsType().getDType())) {
      ps.setDate(parameterIndex, new java.sql.Date(unixTimestamp));
    } else if (DataType.DATETIME.equals(tsCProfile.getCsType().getDType())) {
      ps.setDate(parameterIndex, new java.sql.Date(unixTimestamp));
    } else if (DataType.TIMESTAMP.equals(tsCProfile.getCsType().getDType())) {
      ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  private String getFilterAndString(CProfile cProfileFilter,
                                    String filter) {
    if (cProfileFilter != null) {
      AtomicReference<String> filterAndString = new AtomicReference<>("");

      filterAndString.set(" AND " + cProfileFilter.getColName().toLowerCase() + " = '" + filter + "' ");

      return filterAndString.get();
    } else {
      return "";
    }
  }
}
