package org.fbase.handler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.model.profile.CProfile;

@Log4j2
public class MetadataHandler {

  public static List<CProfile> getCProfileList(Connection connection, String select) throws SQLException {
    List<CProfile> cProfileList = new ArrayList<>();

    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;

    s = connection.createStatement();
    s.executeQuery(select);
    rs = s.getResultSet();
    rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      cProfileList.add(i - 1,
          CProfile.builder()
              .colId(i - 1)
              .colIdSql(i)
              .colName(rsmd.getColumnName(i).toUpperCase())
              .colDbTypeName(rsmd.getColumnTypeName(i).toUpperCase())
              .colSizeDisplay(rsmd.getColumnDisplaySize(i))
              .colSizeSqlType(rsmd.getColumnType(i))
              .build());
    }

    rs.close();
    s.close();

    return cProfileList;
  }

}
