package org.fbase.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.FStore;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;

@Log4j2
public class ClickHouseDatabase implements ClickHouse {
  private final Connection connection;

  @Getter
  private TProfile tProfile;

  @Getter
  private final List<CProfile> cProfileList = new ArrayList<>();

  public ClickHouseDatabase(String url) throws SQLException {
    connection = DriverManager.getConnection(url);
  }

  public List<CProfile> loadDataDirect(String select, FStore fStore, int fBaseBatchSize,
      int resultSetFetchSize) throws SQLException, EnumByteExceedException, SqlColMetadataException {

    List<List<Object>> listsColStore = new ArrayList<>();
    List<CProfile> cProfileList = loadSqlColMetadataList(select);

    cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));

    List<CProfile> cProfiles = cProfileList.stream()
        .map(col -> col.toBuilder()
            .colId(col.getColId())
            .colName(col.getColName())
            .colDbTypeName(col.getColDbTypeName())
            .colSizeDisplay(col.getColSizeDisplay())
            .colSizeSqlType(col.getColSizeSqlType())
            .csType(CSType.builder()
                .isTimeStamp(col.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                .sType(getSType(col.getColName()))
                .build())
            .build()).toList();

    try {
      tProfile = fStore.loadJdbcTableMetadata(connection, select, getSProfile(tableName));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    PreparedStatement ps = connection.prepareStatement(select);
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
          throw new RuntimeException(e);
        }
        if (v.getCsType().isTimeStamp()){
          try {
            long gt = 0;
            if (r.getObject(v.getColIdSql()) instanceof Timestamp ts) {
              gt = ts.getTime();
            } else if (r.getObject(v.getColIdSql()) instanceof LocalDateTime localDateTime) {
              gt = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            if (previousValue[0] == gt) {
              isTheSameKey[0] = true;
            } else {
              previousValue[0] = gt;
            }
          } catch (SQLException sqlException) {
            sqlException.printStackTrace();
          }
        }
      });

      if (cnt.incrementAndGet() >= fBaseBatchSize) {
        log.info("Flush.. " + previousValue[0] + ":" + isTheSameKey[0]);
        if (isTheSameKey[0]) {
          fStore.putDataDirect(tProfile.getTableName(), listsColStore);
          listsColStore.clear();
          cProfileList.forEach(v -> listsColStore.add(v.getColId(), new ArrayList<>()));
          cnt.set(0);
        } else {
          isTheSameKey[0] = false;
        }
      }
    }

    if (cnt.get() != 0) {
      // todo uncomment if you need to create test case for clickhouse FBase05ClickHouseMockTest
      // storeResultSetToFile(cProfileList, listsColStore);
      fStore.putDataDirect(tProfile.getTableName(), listsColStore);
    }

    r.close();
    ps.close();

    return cProfiles;
  }

  public List<CProfile> loadDataJdbcBatch(String select, FStore fStore, int fBaseBatchSize,
      int resultSetFetchSize) throws SQLException, EnumByteExceedException, SqlColMetadataException {

    log.info("Start time: " + LocalDateTime.now());

    List<CProfile> cProfileList = loadSqlColMetadataList(select);

    List<CProfile> cProfiles = cProfileList.stream()
        .map(col -> col.toBuilder()
            .colId(col.getColId())
            .colName(col.getColName())
            .colDbTypeName(col.getColDbTypeName())
            .colSizeDisplay(col.getColSizeDisplay())
            .colSizeSqlType(col.getColSizeSqlType())
            .csType(CSType.builder()
                .isTimeStamp(col.getColName().equalsIgnoreCase("PICKUP_DATETIME"))
                .sType(getSType(col.getColName()))
                .build())
            .build()).toList();

    try {
      tProfile = fStore.loadJdbcTableMetadata(connection, select, getSProfile(tableName));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    PreparedStatement ps = connection.prepareStatement(select);
    ps.setFetchSize(resultSetFetchSize);
    ResultSet r = ps.executeQuery();

    fStore.putDataJdbcBatch(tProfile.getTableName(), r, fBaseBatchSize);

    r.close();
    ps.close();

    log.info("End time: " + LocalDateTime.now());
    return cProfiles;
  }
  
  private void addToList(List<List<Object>> lists, CProfile v, ResultSet r ) throws SQLException {
    lists.get(v.getColId()).add(r.getObject(v.getColIdSql()));
  }

  
  private void storeResultSetToFile(List<CProfile> cProfileList, List<List<Object>> listsColStore)
      throws IOException {
    File f = new File("sqlColProfileList.obj");
    FileOutputStream fos = new FileOutputStream(f);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(cProfileList);
    oos.close();
    fos.close();

    File fData = new File("listsColStore.obj");
    FileOutputStream fosData = new FileOutputStream(fData);
    ObjectOutputStream oosData = new ObjectOutputStream(fosData);
    oosData.writeObject(listsColStore);
    oosData.close();
    fosData.close();
  }

  
  public List<CProfile> loadSqlColMetadataList(String select) throws SQLException {
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
          .colId(i-1)
          .colIdSql(i)
          .colName(rsmd.getColumnName(i).toUpperCase())

          .colDbTypeName(rsmd.getColumnTypeName(i).toUpperCase().contains("(") ?
              rsmd.getColumnTypeName(i).toUpperCase().substring(0, rsmd.getColumnTypeName(i).toUpperCase().indexOf("("))
               : rsmd.getColumnTypeName(i).toUpperCase())

          .colSizeDisplay(rsmd.getColumnDisplaySize(i))
          .colSizeSqlType(rsmd.getColumnType(i))
          .build());
    }

    rs.close();
    s.close();

    return cProfileList;
  }

  
  public void close() throws SQLException {
    connection.close();
  }
}
