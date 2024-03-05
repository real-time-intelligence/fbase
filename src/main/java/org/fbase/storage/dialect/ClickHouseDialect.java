package org.fbase.storage.dialect;

import static org.fbase.storage.helper.ClickHouseHelper.enumParser;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;

public class ClickHouseDialect implements DatabaseDialect {

  @Override
  public String getWhereClass(CProfile tsCProfile,
                              CProfile cProfileFilter,
                              String filter) {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType) ||
        DataType.TIMESTAMP.equals(dataType) ||
        DataType.TIMESTAMPTZ.equals(dataType) ||
        DataType.DATETIME2.equals(dataType) ||
        DataType.SMALLDATETIME.equals(dataType)) {
      return "WHERE " + tsCProfile.getColName().toLowerCase()
          + " BETWEEN ? AND ? " + getFilterAndString(cProfileFilter, filter);
    } else if (DataType.DATETIME.equals(dataType)) {
        return "WHERE " + tsCProfile.getColName().toLowerCase()
            + " BETWEEN toDateTime(?) AND toDateTime(?) " + getFilterAndString(cProfileFilter, filter);
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  @Override
  public String getOrderByClass(CProfile tsCProfile) {
    return " ORDER BY " + tsCProfile.getColName().toLowerCase();
  }

  @Override
  public void setDateTime(CProfile tsCProfile,
                          PreparedStatement ps,
                          int parameterIndex,
                          long unixTimestamp) throws SQLException {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType)) {
      ps.setDate(parameterIndex, new java.sql.Date(unixTimestamp));
    } else if (DataType.DATETIME.equals(dataType)) {
      ps.setLong(parameterIndex, unixTimestamp / 1000);
    } else if (DataType.TIMESTAMP.equals(dataType)
        || DataType.TIMESTAMPTZ.equals(dataType)) {
      ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  private String getFilterAndString(CProfile cProfileFilter,
                                    String filter) {
    if (cProfileFilter != null) {
      AtomicReference<String> filterAndString = new AtomicReference<>("");

      if (cProfileFilter.getColDbTypeName().startsWith("ENUM")) {
        Map<String, Integer> stringIntegerMap = enumParser(cProfileFilter.getColDbTypeName());
        stringIntegerMap.forEach((k, v) -> {
          if (k.equals(filter)) {
            filterAndString.set(" AND " + cProfileFilter.getColName().toLowerCase() + " = '" + v + "' ");
          }
        });
      } else {
        filterAndString.set(" AND " + cProfileFilter.getColName().toLowerCase() + " = '" + filter + "' ");
      }

      return filterAndString.get();
    } else {
      return "";
    }
  }
}
