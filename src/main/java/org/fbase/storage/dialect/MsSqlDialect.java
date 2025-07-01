package org.fbase.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.fbase.metadata.DataType;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.profile.CProfile;
public class MsSqlDialect implements DatabaseDialect {

  @Override
  public String getSelectClassGantt(CProfile firstCProfile,
                                    CProfile secondCProfile) {
    String firstColName = firstCProfile.getColName().toLowerCase();
    String secondColName = secondCProfile.getColName().toLowerCase();

    return "SELECT " + firstColName + ", " + secondColName + ", " +
        "SUM(CASE WHEN " + secondColName + " IS NULL OR " + secondColName + " = '' THEN 1 ELSE 1 END) AS value ";
  }

  @Override
  public String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile) {
    String colName = cProfile.getColName();

    if (GroupFunction.COUNT.equals(groupFunction)) {
      return "SELECT " + colName + ", " +
          "SUM(CASE WHEN " + colName + " IS NULL OR " + colName + " = '' THEN 1 ELSE 1 END) AS value ";
    } else if (GroupFunction.SUM.equals(groupFunction)) {
      return "SELECT '" + colName + "', SUM(" + colName + ") ";
    } else if (GroupFunction.AVG.equals(groupFunction)) {
      return "SELECT '" + colName + "', AVG(" + colName + ") ";
    } else {
      throw new RuntimeException("Not supported");
    }
  }

  @Override
  public String getWhereClass(CProfile tsCProfile,
                              CProfile cProfileFilter,
                              String[] filterData,
                              CompareFunction compareFunction) {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType) ||
        DataType.DATETIME.equals(dataType) ||
        DataType.TIMESTAMP.equals(dataType) ||
        DataType.TIMESTAMPTZ.equals(dataType) ||
        DataType.DATETIME2.equals(dataType) ||
        DataType.SMALLDATETIME.equals(dataType)) {
      return "WHERE " + tsCProfile.getColName().toLowerCase()
          + " BETWEEN ? AND ? " + getFilterAndString(cProfileFilter, filterData, compareFunction);
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  @Override
  public String getOrderByClass(CProfile tsCProfile) {
    return " ORDER BY " + tsCProfile.getColName();
  }

  @Override
  public String getOrderByClass(CProfile cProfile, OrderBy orderBy) {
    return " ORDER BY " + cProfile.getColName() + " " + orderBy.name();
  }

  @Override
  public String getLimitClass(Integer fetchSize) {
    if (fetchSize != null) {
      return " OFFSET 0 ROWS FETCH NEXT " + fetchSize + " ROWS ONLY ";
    }
    return "";
  }

  @Override
  public void setDateTime(CProfile tsCProfile,
                          PreparedStatement ps,
                          int parameterIndex,
                          long unixTimestamp) throws SQLException {
    DataType dataType = tsCProfile.getCsType().getDType();

    if (DataType.DATE.equals(dataType) || DataType.DATETIME.equals(dataType)) {
      ps.setDate(parameterIndex, new java.sql.Date(unixTimestamp));
    } else if (DataType.DATETIME2.equals(dataType)) {
      ps.setTimestamp(parameterIndex, new Timestamp(unixTimestamp));
    } else {
      throw new RuntimeException("Not supported datatype for time-series column: " + tsCProfile.getColName());
    }
  }

  private String getFilterAndString(CProfile cProfileFilter, String[] filterData, CompareFunction compareFunction) {
    if (cProfileFilter == null || filterData == null) {
      return "";
    }

    String columnName = cProfileFilter.getColName();
    StringBuilder filterClause = new StringBuilder();

    for (String filterValue : filterData) {
      String condition;
      if (filterValue == null || filterValue.trim().isEmpty()) {
        condition = columnName + " IS NULL OR " + columnName + " = ''";
      } else {
        String formattedValue = filterValue.trim();
        if (CompareFunction.CONTAIN.equals(compareFunction)) {
          formattedValue = "%" + formattedValue.toLowerCase() + "%";
          condition = "LOWER(" + columnName + ") LIKE '" + formattedValue.replace("'", "''") + "'";
        } else {
          condition = columnName + " = '" + formattedValue.replace("'", "''") + "'";
        }
      }
      if (filterClause.length() > 0) {
        filterClause.append(" OR ");
      }
      filterClause.append(condition);
    }

    return filterClause.length() == 0 ? "" : " AND (" + filterClause.toString() + ")";
  }
}