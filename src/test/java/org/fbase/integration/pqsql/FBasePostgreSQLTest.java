package org.fbase.integration.pqsql;

import com.vividsolutions.jts.util.Assert;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractPostgreSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBasePostgreSQLTest extends AbstractPostgreSQLTest {

  private final String selectAsh = "SELECT current_timestamp as SAMPLE_TIME, "
          + "datid, datname, "
          + "pid AS SESSION_ID, pid AS SESSION_SERIAL, usesysid AS USER_ID, "
          + "coalesce(usename, 'unknown') as usename, "
          + "concat(application_name,'::', backend_type, '::', coalesce(client_hostname, client_addr::text, 'localhost')) AS PROGRAM, "
          + "wait_event_type AS WAIT_CLASS, wait_event AS EVENT, query, substring(md5(query) from 0 for 15) AS SQL_ID, "
          + "left(query, strpos(query, ' '))  AS SQL_OPNAME, "
          + "coalesce(query_start, xact_start, backend_start) as query_start, "
          + "1000 * EXTRACT(EPOCH FROM (clock_timestamp()-coalesce(query_start, xact_start, backend_start))) as duration "
          + "from pg_stat_activity "
          + "where state='active'";
  private final String selectRandom = "SELECT current_timestamp as dt " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_histogram " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_enum " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_raw " +
          "  FROM generate_series(1,50) id " +
          "  ,LATERAL( SELECT nr, ROW_NUMBER() OVER (ORDER BY id * random()) " +
          "             FROM generate_series(1,900) nr " +
          "          ) sub(nr, rn) " +
          "   GROUP BY id";

  private final String selectDataType = "SELECT * FROM pg_dt";

  // TODO Postgresql Data Types: https://www.postgresql.org/docs/current/datatype.html
  List<String> includeList = List.of("bit", "bool", "bytea", "money", "uuid",
          "smallint", "integer", "bigint", "decimal", "numeric", "real", "smallserial", "serial",  "bigserial",
          "int", "int2", "int4", "int8", "float4", "float8", "serial2", "serial4", "serial8",
          "char", "character", "varchar", "text",
          "date", "time", "timetz", "timestamp", "timestamptz");

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

    CProfile cProfileSampleTime = cProfiles.stream().filter(f -> f.getColName().equals("USER_ID")).findAny().get();
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

    loadDataTypes(r, includeList, 0);
  }

  @Test
  public void testDataTypes() throws SQLException, ParseException, BeginEndWrongOrderException, SqlColMetadataException {
    String dropTablePgDt = """
                 DROP TABLE IF EXISTS pg_dt
            """;

    String createTablePgDt = """
                 CREATE TABLE IF NOT EXISTS pg_dt (
                       pg_dt_bit bit,
                       pg_dt_bool bool,
                       pg_dt_bytea bytea,
                       pg_dt_char char,
                       pg_dt_date date,
                       pg_dt_int2 int2,
                       pg_dt_int4 int4,
                       pg_dt_int8 int8,
                       pg_dt_text text,
                       pg_dt_time time,
                       pg_dt_uuid uuid,
                       pg_dt_money money,
                       pg_dt_float4 float4,
                       pg_dt_float8 float8,
                       pg_dt_timetz timetz,
                       pg_dt_varchar varchar,
                       pg_dt_timestamp timestamp,
                       pg_dt_timestamptz timestamptz,
                       pg_dt_smallint smallint
                     )
            """;

    String pg_dt_bit = "0";
    boolean pg_dt_bool = true;
    byte[] pg_dt_bytea = "Test bytea".getBytes(StandardCharsets.UTF_8);
    String pg_dt_char = "A";

    String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Date pg_dt_date = new SimpleDateFormat("yyyy-MM-dd").parse(format);

    short pg_dt_int2 = 123;
    int pg_dt_int4 = 456;
    long pg_dt_int8 = 789L;
    String pg_dt_text = "Some text";
    Time pg_dt_time = Time.valueOf("12:34:56");
    UUID pg_dt_uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    BigDecimal pg_dt_money = new BigDecimal("100.50");
    float pg_dt_float4 = 3.14f;
    double pg_dt_float8 = 3.14159;
    Time pg_dt_timetz = Time.valueOf("12:00:00");
    String pg_dt_varchar = "Hello";
    Timestamp pg_dt_timestamp = Timestamp.valueOf("2023-09-25 13:00:00");
    Timestamp pg_dt_timestamptz = Timestamp.valueOf("2023-09-25 13:00:00");

    Statement createTableStmt = dbConnection.createStatement();
    createTableStmt.executeUpdate(dropTablePgDt);
    createTableStmt.executeUpdate(createTablePgDt);

    String insertQuery = """
            INSERT INTO pg_dt (pg_dt_bit, pg_dt_bool, pg_dt_char, pg_dt_date, pg_dt_int2, pg_dt_int4, pg_dt_int8, 
            pg_dt_text, pg_dt_time, pg_dt_uuid, pg_dt_money, pg_dt_float4, pg_dt_float8, pg_dt_timetz, pg_dt_bytea, 
            pg_dt_varchar, pg_dt_timestamp, pg_dt_timestamptz) 
            VALUES (?::bit, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    PreparedStatement insertStmt = dbConnection.prepareStatement(insertQuery);
    insertStmt.setString(1, pg_dt_bit);
    insertStmt.setBoolean(2, pg_dt_bool);
    insertStmt.setString(3, pg_dt_char);
    insertStmt.setDate(4, new java.sql.Date(pg_dt_date.getTime()));
    insertStmt.setShort(5, pg_dt_int2);
    insertStmt.setInt(6, pg_dt_int4);
    insertStmt.setLong(7, pg_dt_int8);
    insertStmt.setString(8, pg_dt_text);
    insertStmt.setTime(9, pg_dt_time);
    insertStmt.setObject(10, pg_dt_uuid);
    insertStmt.setBigDecimal(11, pg_dt_money);
    insertStmt.setFloat(12, pg_dt_float4);
    insertStmt.setDouble(13, pg_dt_float8);
    insertStmt.setTime(14, pg_dt_timetz);
    insertStmt.setBytes(15, pg_dt_bytea);
    insertStmt.setString(16, pg_dt_varchar);
    insertStmt.setTimestamp(17, pg_dt_timestamp);
    insertStmt.setTimestamp(18, pg_dt_timestamptz);

    insertStmt.executeUpdate();

    Statement selectStmt = dbConnection.createStatement();
    ResultSet resultSet = selectStmt.executeQuery(selectDataType);

    while (resultSet.next()) {
      boolean retrieved_pg_dt_bit = resultSet.getBoolean("pg_dt_bit");
      boolean retrieved_pg_dt_bool = resultSet.getBoolean("pg_dt_bool");
      byte[] retrieved_pg_dt_bytea = resultSet.getBytes("pg_dt_bytea");
      String retrieved_pg_dt_char = resultSet.getString("pg_dt_char");
      Date retrieved_pg_dt_date = resultSet.getDate("pg_dt_date");
      short retrieved_pg_dt_int2 = resultSet.getShort("pg_dt_int2");
      int retrieved_pg_dt_int4 = resultSet.getInt("pg_dt_int4");
      long retrieved_pg_dt_int8 = resultSet.getLong("pg_dt_int8");
      String retrieved_pg_dt_text = resultSet.getString("pg_dt_text");
      Time retrieved_pg_dt_time = resultSet.getTime("pg_dt_time");
      UUID retrieved_pg_dt_uuid = (UUID) resultSet.getObject("pg_dt_uuid");
      BigDecimal retrieved_pg_dt_money = resultSet.getBigDecimal("pg_dt_money");
      float retrieved_pg_dt_float4 = resultSet.getFloat("pg_dt_float4");
      double retrieved_pg_dt_float8 = resultSet.getDouble("pg_dt_float8");
      Time retrieved_pg_dt_timetz = resultSet.getTime("pg_dt_timetz");
      String retrieved_pg_dt_varchar = resultSet.getString("pg_dt_varchar");
      Timestamp retrieved_pg_dt_timestamp = resultSet.getTimestamp("pg_dt_timestamp");
      Timestamp retrieved_pg_dt_timestamptz = resultSet.getTimestamp("pg_dt_timestamptz");

      Assert.equals(pg_dt_bit, retrieved_pg_dt_bit ? "1" : "0");
      Assert.equals(pg_dt_bool, retrieved_pg_dt_bool);
      Assert.equals(new String(pg_dt_bytea, StandardCharsets.UTF_8),  new String(retrieved_pg_dt_bytea, StandardCharsets.UTF_8));
      Assert.equals(pg_dt_char,  retrieved_pg_dt_char);
      Assert.equals(pg_dt_date,  retrieved_pg_dt_date);
      Assert.equals(pg_dt_int2, retrieved_pg_dt_int2);
      Assert.equals(pg_dt_int4, retrieved_pg_dt_int4);
      Assert.equals(pg_dt_int8, retrieved_pg_dt_int8);
      Assert.equals(pg_dt_text, retrieved_pg_dt_text);
      Assert.equals(pg_dt_time, retrieved_pg_dt_time);
      Assert.equals(pg_dt_uuid, retrieved_pg_dt_uuid);
      Assert.equals(pg_dt_money, retrieved_pg_dt_money);
      Assert.equals(pg_dt_float4, retrieved_pg_dt_float4);
      Assert.equals(pg_dt_float8, retrieved_pg_dt_float8);
      Assert.equals(pg_dt_timetz, retrieved_pg_dt_timetz);
      Assert.equals(pg_dt_varchar, retrieved_pg_dt_varchar);
      Assert.equals(pg_dt_timestamp, retrieved_pg_dt_timestamp);
      Assert.equals(pg_dt_timestamptz, retrieved_pg_dt_timestamptz);
    }

    SProfile sProfile = getSProfileForDataTypeTest(selectDataType);

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

    CProfile pgDtBit = getCProfile(cProfiles, "pg_dt_bit");
    CProfile pgDtBool = getCProfile(cProfiles, "pg_dt_bool");
    CProfile pgDtChar = getCProfile(cProfiles, "pg_dt_char");
    CProfile pgDtDate = getCProfile(cProfiles, "pg_dt_date");
    CProfile pgDtInt2 = getCProfile(cProfiles, "pg_dt_int2");
    CProfile pgDtInt4 = getCProfile(cProfiles, "pg_dt_int4");
    CProfile pgDtInt8 = getCProfile(cProfiles, "pg_dt_int8");
    CProfile pgDtText = getCProfile(cProfiles, "pg_dt_text");
    CProfile pgDtTime = getCProfile(cProfiles, "pg_dt_time");
    CProfile pgDtUuid = getCProfile(cProfiles, "pg_dt_uuid");
    CProfile pgDtMoney = getCProfile(cProfiles, "pg_dt_money");
    CProfile pgDtFloat4 = getCProfile(cProfiles, "pg_dt_float4");
    CProfile pgDtFloat8 = getCProfile(cProfiles, "pg_dt_float8");
    CProfile pgDtTimetz = getCProfile(cProfiles, "pg_dt_timetz");
    CProfile pgDtBytea = getCProfile(cProfiles, "pg_dt_bytea");
    CProfile pgDtVarchar = getCProfile(cProfiles, "pg_dt_varchar");
    CProfile pgDtTimestamp = getCProfile(cProfiles, "pg_dt_timestamp");
    CProfile pgDtTimestamptz = getCProfile(cProfiles, "pg_dt_timestamptz");

    List<StackedColumn> stackedColumnsPgDtBit = getStackedColumn(tableName, pgDtBit);
    List<StackedColumn> stackedColumnsPgDtBool = getStackedColumn(tableName, pgDtBool);
    List<StackedColumn> stackedColumnsPgDtChar = getStackedColumn(tableName, pgDtChar);
    List<StackedColumn> stackedColumnsPgDtDate = getStackedColumn(tableName, pgDtDate);
    List<StackedColumn> stackedColumnsPgDtInt2 = getStackedColumn(tableName, pgDtInt2);
    List<StackedColumn> stackedColumnsPgDtInt4 = getStackedColumn(tableName, pgDtInt4);
    List<StackedColumn> stackedColumnsPgDtInt8 = getStackedColumn(tableName, pgDtInt8);
    List<StackedColumn> stackedColumnsPgDtText = getStackedColumn(tableName, pgDtText);
    List<StackedColumn> stackedColumnsPgDtTime = getStackedColumn(tableName, pgDtTime);
    List<StackedColumn> stackedColumnsPgDtUuid = getStackedColumn(tableName, pgDtUuid);
    List<StackedColumn> stackedColumnsPgDtMoney = getStackedColumn(tableName, pgDtMoney);
    List<StackedColumn> stackedColumnsPgDtFloat4 = getStackedColumn(tableName, pgDtFloat4);
    List<StackedColumn> stackedColumnsPgDtFloat8 = getStackedColumn(tableName, pgDtFloat8);
    List<StackedColumn> stackedColumnsPgDtTimetz = getStackedColumn(tableName, pgDtTimetz);
    List<StackedColumn> stackedColumnsPgDtBytea = getStackedColumn(tableName, pgDtBytea);
    List<StackedColumn> stackedColumnsPgDtVarchar = getStackedColumn(tableName, pgDtVarchar);
    List<StackedColumn> stackedColumnsPgDtTimestamp = getStackedColumn(tableName, pgDtTimestamp);
    List<StackedColumn> stackedColumnsPgDtTimestamptz = getStackedColumn(tableName, pgDtTimestamptz);


    System.out.println(stackedColumnsPgDtVarchar);
  }

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow();
  }

  private List<StackedColumn> getStackedColumn(String tableName, CProfile cProfile)
          throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tableName, cProfile, 0, Long.MAX_VALUE);
  }

}
