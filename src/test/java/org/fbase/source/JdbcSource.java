package org.fbase.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Logger;
import org.fbase.core.FStore;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;

public interface JdbcSource {

  default void loadData(FStore fStore, Connection dbConnection, String sqlText,
      SProfile sProfile, Logger log, int fBaseBatchSize, int resultSetFetchSize) throws SQLException {

    TProfile tProfile = fStore.loadJdbcTableMetadata(dbConnection, sqlText, sProfile);

    List<CProfile> cProfiles = fStore.getCProfileList(tProfile);

    List<List<Object>> listsColStore = new ArrayList<>();
    cProfiles.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    PreparedStatement ps = dbConnection.prepareStatement(sqlText);
    ResultSet r = ps.executeQuery();
    r.setFetchSize(resultSetFetchSize);

    final boolean[] isTheSameKey = {false};
    final long[] previousValue = {0};
    AtomicInteger cnt = new AtomicInteger(0);

    while (r.next()) {
      cProfiles.forEach(v -> {

        try {
          addToList(listsColStore, v, r);
        } catch (SQLException e) {
          log.catching(e);
          throw new RuntimeException(e);
        }

        if (v.getCsType().isTimeStamp()) {
          try {
            Timestamp dt = (Timestamp) r.getObject(v.getColIdSql());
            if (previousValue[0] == dt.getTime()) {
              isTheSameKey[0] = true;
            } else {
              previousValue[0] = dt.getTime();
            }
          } catch (SQLException throwables) {
            throwables.printStackTrace();
          }
        }
      });

      if (cnt.incrementAndGet() >= fBaseBatchSize) {
        log.info("Flush.. " + previousValue[0] + ":" + isTheSameKey[0]);
        if (isTheSameKey[0]) {
          try {
            fStore.putDataDirect(tProfile, listsColStore);
          } catch (Exception e) {
            log.catching(e);
          }
          listsColStore.clear();
          cProfiles.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));
          cnt.set(0);
        } else {
          isTheSameKey[0] = false;
        }
        log.info("Flush ended.. ");
      }
    }

    if (cnt.get() != 0) {
      try {
        fStore.putDataDirect(tProfile, listsColStore);
      } catch (Exception e) {
        log.catching(e);
      }
    }

    r.close();
    ps.close();
  }

  default void addToList(List<List<Object>> lists, CProfile v, ResultSet r) throws SQLException {
    lists.get(v.getColId()).add(r.getObject(v.getColIdSql()));
  }

  default SProfile getSProfileForAsh(String select, Connection dbConnection) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;

    s = dbConnection.createStatement();
    s.executeQuery(select);
    rs = s.getResultSet();
    rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        csTypeMap.put(rsmd.getColumnName(i).toUpperCase(), new CSType().toBuilder().build());
    }

    rs.close();
    s.close();

    return new SProfile().setCsTypeMap(csTypeMap);
  }
}
