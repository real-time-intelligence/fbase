package org.fbase.integration.oracle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.vividsolutions.jts.util.Assert;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractOracleTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseOracleTest extends AbstractOracleTest {

  private final String selectAsh = "SELECT * FROM v$active_session_history FETCH FIRST 10 ROWS ONLY";
  private final String selectRandom = "SELECT SYSDATE AS dt, "
      + "value AS value_histogram, "
      + "value AS value_enum, "
      + "value AS value_raw"
      + " FROM (SELECT (MOD(Round(DBMS_RANDOM.Value(1, 99)), 9) + 1) value FROM dual )";

  // TODO Oracle Data Types: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
  List<String> includeList = List.of("VARCHAR2", "NVARCHAR2", "NUMBER", "FLOAT", /*"LONG",*/
      /*"DATE", "TIMESTAMP",*/ "RAW", /*"TIMESTAMP WITH TIME ZONE",*/ "ROWID", "UROWID", /*"CHAR",*/
      "NCHAR", "CLOB", "NCLOB");

  List<String> includeListAll = List.of("VARCHAR2", "NVARCHAR2", "NUMBER", "FLOAT", "LONG",
      "DATE", "TIMESTAMP", "RAW", "TIMESTAMP WITH TIME ZONE", "ROWID", "UROWID", "CHAR",
      "NCHAR", "CLOB", "NCLOB");

  private final String selectDataType = "SELECT * FROM oracle_data_types";

  @BeforeAll
  public void initialLoading() {
    try {
      loadData(fStore, dbConnection, selectRandom, getSProfileForRandom(), log,20000, 20000);
      loadData(fStore, dbConnection, selectAsh, getSProfileForAsh(selectAsh), log,20000, 20000);
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void selectRandomTest()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {
    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectRandom, getSProfileForRandom());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileHistogram = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_HISTOGRAM")).findAny().get();
    CProfile cProfileEnum = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_ENUM")).findAny().get();
    CProfile cProfileRaw = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_RAW")).findAny().get();

    List<StackedColumn> stackedColumnsHistogram =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileHistogram, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsEnum =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileEnum, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsRaw =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileRaw, 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsHistogram);
    System.out.println(stackedColumnsEnum);
    System.out.println(stackedColumnsRaw);
  }

  @Test
  public void selectAshTest()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {

    SProfile sProfile = getSProfileForAsh(selectAsh);

    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectAsh, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileSampleTime = cProfiles.stream().filter(f -> f.getColName().equals("SAMPLE_ID")).findAny().get();
    CProfile cProfileSqlId = cProfiles.stream().filter(f -> f.getColName().equals("SQL_ID")).findAny().get();
    CProfile cProfileEvent = cProfiles.stream().filter(f -> f.getColName().equals("EVENT")).findAny().get();

    List<StackedColumn> stackedColumnsBySampleTime =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSampleTime, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsBySqlId =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSqlId, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByEvent =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileEvent, 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsBySampleTime);
    System.out.println(stackedColumnsBySqlId);
    System.out.println(stackedColumnsByEvent);
  }

  @Test
  public void loadDataTypes() throws SQLException {
    ResultSet r = dbConnection.getMetaData().getTypeInfo();

    loadDataTypes(r, includeListAll, 23);
  }

  @Test
  public void testDataTypes() throws SQLException, ParseException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    String createTableOracleDt = """
             CREATE TABLE oracle_data_types (
                oracle_dt_raw RAW(23),
                oracle_dt_char CHAR(24),
                oracle_dt_clob CLOB,
                oracle_dt_date DATE,
                oracle_dt_float FLOAT,
                oracle_dt_nchar NCHAR,
                oracle_dt_nclob NCLOB,
                oracle_dt_number NUMBER,
                oracle_dt_varchar2 VARCHAR2(32),
                oracle_dt_nvarchar2 NVARCHAR2(33),
                oracle_dt_timestamp TIMESTAMP(6)
                )
        """;

    byte[] raw = "Test bytea".getBytes(StandardCharsets.UTF_8);
    String charVal = "Sample Char";
    String clob = "Sample CLOB";
    java.sql.Date date = java.sql.Date.valueOf("2023-10-10");
    long pg_dt_date_long = date.getTime();
    float floatVal = 123.45f;
    String nchar = "S";
    String nclob = "B";
    int number = 12345;
    String varchar2 = "Sample VARCHAR2";
    String nvarchar2 = "Sample NVARCHAR2";
    java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2023-10-10 12:00:00");

    Statement createTableStmt = dbConnection.createStatement();

    String tableNameOracle = "ORACLE_DATA_TYPES";
    if (tableExists(dbConnection, tableNameOracle)) {
      dropTable(dbConnection, tableNameOracle);
    } else {
      log.info("Skip drop operation, table not exist in DB..");
    }
    createTableStmt.executeUpdate(createTableOracleDt);

    String insertQuery = """
         INSERT INTO oracle_data_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    PreparedStatement insertStmt = dbConnection.prepareStatement(insertQuery);

    insertStmt.setBytes(1, raw);
    insertStmt.setString(2, charVal);
    insertStmt.setString(3, clob);
    insertStmt.setDate(4, date);
    insertStmt.setFloat(5, floatVal);
    insertStmt.setString(6, nchar);
    insertStmt.setString(7, nclob);
    insertStmt.setInt(8, number);
    insertStmt.setString(9, varchar2);
    insertStmt.setString(10, nvarchar2);
    insertStmt.setTimestamp(11, timestamp);

    insertStmt.executeUpdate();

    Statement selectStmt = dbConnection.createStatement();
    ResultSet resultSet = selectStmt.executeQuery(selectDataType);

    while (resultSet.next()) {
      byte[] retrieved_raw = resultSet.getBytes("oracle_dt_raw");
      String retrieved_charVal = resultSet.getString("oracle_dt_char");
      String retrieved_clob = resultSet.getString("oracle_dt_clob");
      java.sql.Date retrieved_date = resultSet.getDate("oracle_dt_date");
      float retrieved_floatVal = resultSet.getFloat("oracle_dt_float");
      String retrieved_nchar = resultSet.getString("oracle_dt_nchar");
      String retrieved_nclob = resultSet.getString("oracle_dt_nclob");
      int retrieved_number = resultSet.getInt("oracle_dt_number");
      String retrieved_varchar2 = resultSet.getString("oracle_dt_varchar2");
      String retrieved_nvarchar2 = resultSet.getString("oracle_dt_nvarchar2");
      java.sql.Timestamp retrieved_timestamp = resultSet.getTimestamp("oracle_dt_timestamp");

      assertArrayEquals(raw, retrieved_raw);
      assertEquals(charVal, retrieved_charVal.trim());
      assertEquals(clob, retrieved_clob);
      assertEquals(date, retrieved_date);
      assertEquals(floatVal, retrieved_floatVal);
      assertEquals(nchar, retrieved_nchar);
      assertEquals(nclob, retrieved_nclob);
      assertEquals(number, retrieved_number);
      assertEquals(varchar2, retrieved_varchar2);
      assertEquals(nvarchar2, retrieved_nvarchar2);
      assertEquals(timestamp, retrieved_timestamp);
    }

    SProfile sProfile = getSProfile(selectDataType);

    loadData(fStore, dbConnection, selectDataType, sProfile, log,20000, 20000);

    TProfile tProfile;
    String tableName;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectDataType, sProfile);
      tableName = tProfile.getTableName();
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile oracleDtRaw = getCProfile(cProfiles, "oracle_dt_raw");
    CProfile oracleDtChar = getCProfile(cProfiles, "oracle_dt_char");
    CProfile oracleDtClob = getCProfile(cProfiles, "oracle_dt_clob");
    CProfile oracleDtDate = getCProfile(cProfiles, "oracle_dt_date");
    CProfile oracleDtFloat = getCProfile(cProfiles, "oracle_dt_float");
    CProfile oracleDtNchar = getCProfile(cProfiles, "oracle_dt_nchar");
    CProfile oracleDtNclob = getCProfile(cProfiles, "oracle_dt_nclob");
    CProfile oracleDtNumber = getCProfile(cProfiles, "oracle_dt_number");
    CProfile oracleDtVarchar2 = getCProfile(cProfiles, "oracle_dt_varchar2");
    CProfile oracleDtNvarchar2 = getCProfile(cProfiles, "oracle_dt_nvarchar2");
    CProfile oracleDtTimestamp = getCProfile(cProfiles, "oracle_dt_timestamp");

    /* Test StackedColumn API */
    Assert.equals(new String(raw, StandardCharsets.UTF_8), getStackedColumnKey(tableName, oracleDtRaw));
    Assert.equals(charVal, getStackedColumnKey(tableName, oracleDtChar));
    Assert.equals(clob, getStackedColumnKey(tableName, oracleDtClob));
    Assert.equals(pg_dt_date_long, Long.valueOf(getStackedColumnKey(tableName, oracleDtDate)));
    Assert.equals(floatVal, Float.valueOf(getStackedColumnKey(tableName, oracleDtFloat)));
    Assert.equals(nchar, getStackedColumnKey(tableName, oracleDtNchar));
    Assert.equals(nclob, getStackedColumnKey(tableName, oracleDtNclob));
    Assert.equals(number, Integer.valueOf(getStackedColumnKey(tableName, oracleDtNumber)));
    Assert.equals(varchar2, getStackedColumnKey(tableName, oracleDtVarchar2));
    Assert.equals(nvarchar2, getStackedColumnKey(tableName, oracleDtNvarchar2));
    //Assert.equals(timestamp, getStackedColumnKey(tableName, oracleDtTimestamp)); Not supported for timestamp column..

    /* Test GanttColumn API */



    /* Test Raw data API */



  }

  protected SProfile getSProfile(String select) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, dbConnection).getCsTypeMap().forEach((key, value) -> {
      if (key.equalsIgnoreCase("oracle_dt_timestamp")) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.RAW).build());
      }
    });

    return new SProfile().setTableName(tableNameDataType)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.GLOBAL)
        .setCompression(false)
        .setCsTypeMap(csTypeMap);
  }

  private static boolean tableExists(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    try (var resultSet = metaData.getTables(null, null, tableName, null)) {
      return resultSet.next();
    }
  }

  private static void dropTable(Connection connection, String tableName) throws SQLException {
    String sql = "DROP TABLE " + tableName;

    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow();
  }

  private String getStackedColumnKey(String tableName, CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tableName, cProfile, 0, Long.MAX_VALUE)
        .stream()
        .findAny()
        .orElseThrow()
        .getKeyCount()
        .entrySet()
        .stream()
        .findAny()
        .orElseThrow()
        .getKey();
  }

}
