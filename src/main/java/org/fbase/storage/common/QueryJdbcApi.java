package org.fbase.storage.common;

import static org.fbase.service.mapping.Mapper.convertRawToLong;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.dialect.DatabaseDialect;

public abstract class QueryJdbcApi {

  private final BasicDataSource basicDataSource;

  protected QueryJdbcApi(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  protected List<StackedColumn> getListStackedColumn(String tableName,
                                                     CProfile tsCProfile,
                                                     CProfile cProfile,
                                                     GroupFunction groupFunction,
                                                     CProfile cProfileFilter,
                                                     String filter,
                                                     long begin,
                                                     long end,
                                                     DatabaseDialect databaseDialect) {
    List<StackedColumn> results = new ArrayList<>();

    String colName = cProfile.getColName().toLowerCase();

    String query =
            "SELECT " + colName + ", COUNT(" + colName + ") " +
            "FROM " + tableName + " " +
              databaseDialect.getWhereClass(tsCProfile, cProfileFilter, filter) +
            "GROUP BY " + colName;

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
      databaseDialect.setDateTime(tsCProfile, ps, 2, end);

      ResultSet rs = ps.executeQuery();

      StackedColumn column = new StackedColumn();
      column.setKey(begin);
      column.setTail(end);

      while (rs.next()) {
        String key = rs.getString(1);
        int count = rs.getInt(2);

        column.getKeyCount().put(key, count);
      }

      results.add(column);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return results;
  }

  protected List<GanttColumn> getListGanttColumn(String tableName,
                                                 CProfile tsCProfile,
                                                 CProfile firstGrpBy,
                                                 CProfile secondGrpBy,
                                                 long begin,
                                                 long end,
                                                 DatabaseDialect databaseDialect) {
    List<GanttColumn> ganttColumns = new ArrayList<>();

    String firstColName = firstGrpBy.getColName().toLowerCase();
    String secondColName = secondGrpBy.getColName().toLowerCase();

    String query =
        "SELECT " + firstColName + ", " + secondColName + ", COUNT(" + secondColName + ") " +
            "FROM " + tableName + " " +
            databaseDialect.getWhereClass(tsCProfile, null, null) +
            "GROUP BY " + firstColName + ", " + secondColName;

    Map<String, Map<String, Integer>> map = new LinkedHashMap<>();

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
      databaseDialect.setDateTime(tsCProfile, ps, 2, end);

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        String key = rs.getString(1);
        String keyGantt = rs.getString(2);
        int countGantt = rs.getInt(3);

        map.computeIfAbsent(key, k -> new HashMap<>()).put(keyGantt, countGantt);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    for (Map.Entry<String, Map<String, Integer>> entry : map.entrySet()) {
      GanttColumn column = new GanttColumn(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
      ganttColumns.add(column);
    }

    return ganttColumns;
  }

  protected long getLastBlockIdLocal(String tableName,
                                     CProfile tsCProfile,
                                     long begin,
                                     long end,
                                     DatabaseDialect databaseDialect) {
    long lastBlockId = 0L;

    String query = "";

    String colName = tsCProfile.getColName().toLowerCase();
    if (Long.MAX_VALUE == end) {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName;
    } else {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName + " " +
              databaseDialect.getWhereClass(tsCProfile, null, null);
    }

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      if (Long.MAX_VALUE == end) {

      } else {
        databaseDialect.setDateTime(tsCProfile, ps, 1, begin);
        databaseDialect.setDateTime(tsCProfile, ps, 2, end);
      }

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        Object object = rs.getObject(1);

        lastBlockId = convertRawToLong(object, tsCProfile);
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return lastBlockId;
  }
}
