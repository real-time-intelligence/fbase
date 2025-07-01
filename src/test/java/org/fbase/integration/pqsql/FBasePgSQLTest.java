package org.fbase.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.RoundingMode;
import java.text.DateFormat;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractPostgreSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumnCount;
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
import org.postgresql.geometric.PGpoint;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBasePgSQLTest extends AbstractPostgreSQLTest {

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
          "char", "character", "varchar", "text", "bpchar",
          "date", "time", "timetz", "timestamp", "timestamptz", "jsonb", "point");

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
        fStore.getStacked(tProfile.getTableName(), cProfileHistogram, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsEnum =
        fStore.getStacked(tProfile.getTableName(), cProfileEnum, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsRaw =
        fStore.getStacked(tProfile.getTableName(), cProfileRaw, GroupFunction.COUNT, 0, Long.MAX_VALUE);

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
        fStore.getStacked(tProfile.getTableName(), cProfileSampleTime, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsBySqlId =
        fStore.getStacked(tProfile.getTableName(), cProfileSqlId, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByEvent =
        fStore.getStacked(tProfile.getTableName(), cProfileEvent, GroupFunction.COUNT, 0, Long.MAX_VALUE);

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
  public void testDataTypes()
      throws SQLException, ParseException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    String dropTableDt = """
                 DROP TABLE IF EXISTS pg_dt
            """;

    String createTableDt = """
                 CREATE TABLE IF NOT EXISTS pg_dt (
                       pg_dt_bit bit,
                       pg_dt_bool bool,
                       pg_dt_char char,
                       pg_dt_bpchar bpchar,
                       pg_dt_date date,
                       pg_dt_int2 int2,
                       pg_dt_int4 int4,
                       pg_dt_int8 int8,
                       pg_dt_text text,
                       pg_dt_time time,
                       pg_dt_uuid uuid,
                       pg_dt_bytea bytea,
                       pg_dt_money money,
                       pg_dt_float4 float4,
                       pg_dt_float8 float8,
                       pg_dt_serial serial,
                       pg_dt_numeric numeric,
                       pg_dt_varchar varchar,
                       pg_dt_bigserial bigserial,
                       pg_dt_smallserial smallserial,
                       pg_dt_timestamp timestamp,
                       pg_dt_timetz timetz,
                       pg_dt_timestamptz timestamptz,
                       pg_dt_jsonb jsonb,
                       pg_dt_point point,
                       pg_dt_interval interval
                     )
            """;

    String pg_dt_bit = "0";
    boolean pg_dt_bool = true;
    byte[] pg_dt_bytea = "Test bytea".getBytes(StandardCharsets.UTF_8);
    String pg_dt_char = "A";
    String pg_dt_bpchar = "B";

    String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Date pg_dt_date = new SimpleDateFormat("yyyy-MM-dd").parse(format);
    long pg_dt_date_long = pg_dt_date.getTime();

    short pg_dt_int2 = 123;
    int pg_dt_int4 = 456;
    long pg_dt_int8 = 789L;
    String pg_dt_text = "Some text";
    Time pg_dt_time = Time.valueOf("12:34:56");
    int pg_dt_time_int = Math.toIntExact(pg_dt_time.getTime());
    UUID pg_dt_uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    BigDecimal pg_dt_money = new BigDecimal("100.51");
    float pg_dt_float4 = 3.14f;
    double pg_dt_float8 = 3.14159;
    long pg_dt_serial = 10L;
    long pg_dt_bigserial = pg_dt_serial - 1;
    long pg_dt_smallserial = pg_dt_serial + 2;
    BigDecimal pg_dt_numeric = BigDecimal.valueOf(0.23);

    Time pg_dt_timetz = Time.valueOf("12:00:10");
    int pg_dt_timetz_int = Math.toIntExact(pg_dt_timetz.getTime());
    String pg_dt_varchar = "Hello";

    String pg_dt_timestamp_string = "2023-09-25 13:00:00";
    Timestamp pg_dt_timestamp = Timestamp.valueOf(pg_dt_timestamp_string);
    Timestamp pg_dt_timestamptz = Timestamp.valueOf(pg_dt_timestamp_string);

    String pg_dt_jsonb_string = """ 
        {"age": 30, "city": "New York", "name": "John Doe"}
        """.trim();
    byte[] pg_dt_jsonb = pg_dt_jsonb_string.getBytes(StandardCharsets.UTF_8);

    PGpoint pg_dt_point = new PGpoint(123, 456);
    String pg_dt_interval = "0 years 0 mons 5 days 10 hours 0 mins 0.0 secs";

    Statement createTableStmt = dbConnection.createStatement();
    createTableStmt.executeUpdate(dropTableDt);
    createTableStmt.executeUpdate(createTableDt);

    String insertQuery = """
         INSERT INTO pg_dt (pg_dt_bit, pg_dt_bool, pg_dt_char, pg_dt_bpchar, pg_dt_date, pg_dt_int2, pg_dt_int4, pg_dt_int8, 
         pg_dt_text, pg_dt_time, pg_dt_uuid, pg_dt_bytea, pg_dt_money, pg_dt_float4, pg_dt_float8, 
         pg_dt_serial, pg_dt_timetz, pg_dt_numeric, pg_dt_varchar, pg_dt_bigserial, pg_dt_timestamp, 
         pg_dt_smallserial, pg_dt_timestamptz, pg_dt_jsonb, pg_dt_point, pg_dt_interval) 
         VALUES (?::bit, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::json, ?, ?::interval)
            """;

    try (PreparedStatement ps = dbConnection.prepareStatement(insertQuery)) {
      ps.setString(1, pg_dt_bit);
      ps.setBoolean(2, pg_dt_bool);
      ps.setString(3, pg_dt_char);
      ps.setString(4, pg_dt_bpchar);
      ps.setDate(5, new java.sql.Date(pg_dt_date.getTime()));
      ps.setShort(6, pg_dt_int2);
      ps.setInt(7, pg_dt_int4);
      ps.setLong(8, pg_dt_int8);
      ps.setString(9, pg_dt_text);
      ps.setTime(10, pg_dt_time);
      ps.setObject(11, pg_dt_uuid);
      ps.setBytes(12, pg_dt_bytea);
      ps.setBigDecimal(13, pg_dt_money);
      ps.setFloat(14, pg_dt_float4);
      ps.setDouble(15, pg_dt_float8);
      ps.setLong(16, pg_dt_serial);
      ps.setTime(17, pg_dt_timetz);
      ps.setBigDecimal(18, pg_dt_numeric);
      ps.setString(19, pg_dt_varchar);
      ps.setLong(20, pg_dt_bigserial);
      ps.setTimestamp(21, pg_dt_timestamp);
      ps.setLong(22, pg_dt_smallserial);
      ps.setTimestamp(23, pg_dt_timestamptz);
      ps.setObject(24, pg_dt_jsonb_string, java.sql.Types.OTHER);
      ps.setObject(25, pg_dt_point);
      ps.setString(26, pg_dt_interval);

      ps.executeUpdate();
    }

    Statement selectStmt = dbConnection.createStatement();
    ResultSet resultSet = selectStmt.executeQuery(selectDataType);

    while (resultSet.next()) {
      boolean retrieved_pg_dt_bit = resultSet.getBoolean("pg_dt_bit");
      boolean retrieved_pg_dt_bool = resultSet.getBoolean("pg_dt_bool");
      String retrieved_pg_dt_char = resultSet.getString("pg_dt_char");
      String retrieved_pg_dt_bpchar = resultSet.getString("pg_dt_bpchar");
      Date retrieved_pg_dt_date = resultSet.getDate("pg_dt_date");
      short retrieved_pg_dt_int2 = resultSet.getShort("pg_dt_int2");
      int retrieved_pg_dt_int4 = resultSet.getInt("pg_dt_int4");
      long retrieved_pg_dt_int8 = resultSet.getLong("pg_dt_int8");
      String retrieved_pg_dt_text = resultSet.getString("pg_dt_text");
      Time retrieved_pg_dt_time = resultSet.getTime("pg_dt_time");
      UUID retrieved_pg_dt_uuid = (UUID) resultSet.getObject("pg_dt_uuid");
      byte[] retrieved_pg_dt_bytea = resultSet.getBytes("pg_dt_bytea");
      BigDecimal retrieved_pg_dt_money = resultSet.getBigDecimal("pg_dt_money");
      float retrieved_pg_dt_float4 = resultSet.getFloat("pg_dt_float4");
      double retrieved_pg_dt_float8 = resultSet.getDouble("pg_dt_float8");
      long retrieved_pg_dt_serial = resultSet.getLong("pg_dt_serial");
      Time retrieved_pg_dt_timetz = resultSet.getTime("pg_dt_timetz");
      BigDecimal retrieved_pg_dt_numeric = resultSet.getBigDecimal("pg_dt_numeric");
      String retrieved_pg_dt_varchar = resultSet.getString("pg_dt_varchar");
      long retrieved_pg_dt_bigserial = resultSet.getLong("pg_dt_bigserial");
      Timestamp retrieved_pg_dt_timestamp = resultSet.getTimestamp("pg_dt_timestamp");
      long retrieved_pg_dt_smallserial = resultSet.getLong("pg_dt_smallserial");
      Timestamp retrieved_pg_dt_timestamptz = resultSet.getTimestamp("pg_dt_timestamptz");
      byte[] retrieved_pg_dt_jsonb = resultSet.getBytes("pg_dt_jsonb");
      PGpoint retrieved_pg_dt_point = (PGpoint) resultSet.getObject("pg_dt_point");
      Object retrieved_pg_dt_interval = resultSet.getObject("pg_dt_interval");

      assertEquals(pg_dt_bit, retrieved_pg_dt_bit ? "1" : "0");
      assertEquals(pg_dt_bool, retrieved_pg_dt_bool);
      assertEquals(pg_dt_char,  retrieved_pg_dt_char);
      assertEquals(pg_dt_bpchar,  retrieved_pg_dt_bpchar);
      assertEquals(pg_dt_date,  retrieved_pg_dt_date);
      assertEquals(pg_dt_int2, retrieved_pg_dt_int2);
      assertEquals(pg_dt_int4, retrieved_pg_dt_int4);
      assertEquals(pg_dt_int8, retrieved_pg_dt_int8);
      assertEquals(pg_dt_text, retrieved_pg_dt_text);
      assertEquals(pg_dt_time, retrieved_pg_dt_time);
      assertEquals(pg_dt_uuid, retrieved_pg_dt_uuid);
      assertEquals(new String(pg_dt_bytea, StandardCharsets.UTF_8),  new String(retrieved_pg_dt_bytea, StandardCharsets.UTF_8));
      assertEquals(pg_dt_money, retrieved_pg_dt_money);
      assertEquals(pg_dt_float4, retrieved_pg_dt_float4);
      assertEquals(pg_dt_float8, retrieved_pg_dt_float8);
      assertEquals(pg_dt_serial, retrieved_pg_dt_serial);
      assertEquals(pg_dt_timetz, retrieved_pg_dt_timetz);
      assertEquals(pg_dt_numeric, retrieved_pg_dt_numeric);
      assertEquals(pg_dt_varchar, retrieved_pg_dt_varchar);
      assertEquals(pg_dt_bigserial, retrieved_pg_dt_bigserial);
      assertEquals(pg_dt_timestamp, retrieved_pg_dt_timestamp);
      assertEquals(pg_dt_smallserial, retrieved_pg_dt_smallserial);
      assertEquals(pg_dt_timestamptz, retrieved_pg_dt_timestamptz);
      assertEquals(new String(pg_dt_jsonb, StandardCharsets.UTF_8),  new String(retrieved_pg_dt_jsonb, StandardCharsets.UTF_8));
      assertEquals(pg_dt_point, retrieved_pg_dt_point);
      assertEquals(pg_dt_interval, retrieved_pg_dt_interval.toString());
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
    CProfile pgDtBpChar = getCProfile(cProfiles, "pg_dt_bpchar");
    CProfile pgDtDate = getCProfile(cProfiles, "pg_dt_date");
    CProfile pgDtInt2 = getCProfile(cProfiles, "pg_dt_int2");
    CProfile pgDtInt4 = getCProfile(cProfiles, "pg_dt_int4");
    CProfile pgDtInt8 = getCProfile(cProfiles, "pg_dt_int8");
    CProfile pgDtText = getCProfile(cProfiles, "pg_dt_text");
    CProfile pgDtTime = getCProfile(cProfiles, "pg_dt_time");
    CProfile pgDtUuid = getCProfile(cProfiles, "pg_dt_uuid");
    CProfile pgDtBytea = getCProfile(cProfiles, "pg_dt_bytea");
    CProfile pgDtMoney = getCProfile(cProfiles, "pg_dt_money");
    CProfile pgDtFloat4 = getCProfile(cProfiles, "pg_dt_float4");
    CProfile pgDtFloat8 = getCProfile(cProfiles, "pg_dt_float8");
    CProfile pgDtSerial = getCProfile(cProfiles, "pg_dt_serial");
    CProfile pgDtTimetz = getCProfile(cProfiles, "pg_dt_timetz");
    CProfile pgDtNumeric = getCProfile(cProfiles, "pg_dt_numeric");
    CProfile pgDtVarchar = getCProfile(cProfiles, "pg_dt_varchar");
    CProfile pgDtBigserial = getCProfile(cProfiles, "pg_dt_bigserial");
    CProfile pgDtTimestamp = getCProfile(cProfiles, "pg_dt_timestamp");
    CProfile pgDtSmallserial = getCProfile(cProfiles, "pg_dt_smallserial");
    CProfile pgDtTimestamptz = getCProfile(cProfiles, "pg_dt_timestamptz");
    CProfile pgDtJsonb = getCProfile(cProfiles, "pg_dt_jsonb");
    CProfile pgDtPoint = getCProfile(cProfiles, "pg_dt_point");
    CProfile pgDtInterval = getCProfile(cProfiles, "pg_dt_interval");

    /* Test StackedColumn API */
    assertEquals(pg_dt_bit, getStackedColumnKey(tableName, pgDtBit));
    assertEquals(String.valueOf(pg_dt_bool), getStackedColumnKey(tableName, pgDtBool));
    assertEquals(pg_dt_char, getStackedColumnKey(tableName, pgDtChar));
    assertEquals(pg_dt_bpchar, getStackedColumnKey(tableName, pgDtBpChar));
    assertEquals(pg_dt_date_long, Long.parseLong(getStackedColumnKey(tableName, pgDtDate)));
    assertEquals(pg_dt_int2, Short.parseShort(getStackedColumnKey(tableName, pgDtInt2)));
    assertEquals(pg_dt_int4, Integer.parseInt(getStackedColumnKey(tableName, pgDtInt4)));
    assertEquals(pg_dt_int8, Long.parseLong(getStackedColumnKey(tableName, pgDtInt8)));
    assertEquals(pg_dt_text, getStackedColumnKey(tableName, pgDtText));
    assertEquals(pg_dt_time_int, Integer.parseInt(getStackedColumnKey(tableName, pgDtTime)));
    assertEquals(pg_dt_uuid.toString(), getStackedColumnKey(tableName, pgDtUuid));
    assertEquals(new String(pg_dt_bytea, StandardCharsets.UTF_8), getStackedColumnKey(tableName, pgDtBytea));
    assertEquals(pg_dt_money, new BigDecimal(getStackedColumnKey(tableName, pgDtMoney)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(new BigDecimal(pg_dt_float4).setScale(2, RoundingMode.HALF_UP),
        new BigDecimal(getStackedColumnKey(tableName, pgDtFloat4)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(new BigDecimal(pg_dt_float8).setScale(2, RoundingMode.HALF_UP),
        new BigDecimal(getStackedColumnKey(tableName, pgDtFloat8)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(pg_dt_serial, Long.parseLong(getStackedColumnKey(tableName, pgDtSerial)));
    assertEquals(pg_dt_smallserial, Long.parseLong(getStackedColumnKey(tableName, pgDtSmallserial)));
    assertEquals(pg_dt_bigserial, Long.parseLong(getStackedColumnKey(tableName, pgDtBigserial)));
    assertEquals(pg_dt_timetz_int, Integer.parseInt(getStackedColumnKey(tableName, pgDtTimetz)));
    assertEquals(pg_dt_numeric, new BigDecimal(getStackedColumnKey(tableName, pgDtNumeric)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(pg_dt_varchar, getStackedColumnKey(tableName, pgDtVarchar));

    DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    /** Not supported for timestamp column
    Date date = formatter.parse(getStackedColumnKey(tableName, pgDtTimestamp));
    assertEquals(pg_dt_timestamp.getTime(), date.getTime());
    */
    Date date = formatter.parse(getStackedColumnKey(tableName, pgDtTimestamptz));
    assertEquals(pg_dt_timestamptz.getTime(), date.getTime());
    assertEquals(pg_dt_jsonb_string, getStackedColumnKey(tableName, pgDtJsonb));
    assertEquals(pg_dt_point.getValue(), getStackedColumnKey(tableName, pgDtPoint));
    assertEquals(pg_dt_interval, getStackedColumnKey(tableName, pgDtInterval));

    /* Test GanttColumn API */
    List<GanttColumnCount> pgDtBitPgDtBool = getGanttColumn(tableName, pgDtBit, pgDtBool);
    assertEquals(pg_dt_bit, pgDtBitPgDtBool.get(0).getKey());
    assertEquals(pg_dt_bool, pgDtBitPgDtBool.get(0).getGantt().containsKey(String.valueOf(pg_dt_bool)));

    List<GanttColumnCount> pgDtBoolPgDtChar = getGanttColumn(tableName, pgDtBool, pgDtChar);
    assertEquals(String.valueOf(pg_dt_bool), pgDtBoolPgDtChar.get(0).getKey());
    assertEquals(pg_dt_char, getGanttKey(pgDtBoolPgDtChar, pg_dt_char));

    List<GanttColumnCount> pgDtCharPgDtBpChar = getGanttColumn(tableName, pgDtChar, pgDtBpChar);
    assertEquals(pg_dt_char, pgDtCharPgDtBpChar.get(0).getKey());
    assertEquals(pg_dt_bpchar, getGanttKey(pgDtCharPgDtBpChar, pg_dt_bpchar));

    List<GanttColumnCount> pgDtBpCharPgDtDate = getGanttColumn(tableName, pgDtBpChar, pgDtDate);
    assertEquals(pg_dt_bpchar, pgDtBpCharPgDtDate.get(0).getKey());
    assertEquals(pg_dt_date_long, Long.parseLong(getGanttKey(pgDtBpCharPgDtDate, String.valueOf(pg_dt_date_long))));

    List<GanttColumnCount> pgDtDatePgDtInt2 = getGanttColumn(tableName, pgDtDate, pgDtInt2);
    assertEquals(pg_dt_date_long, Long.parseLong(pgDtDatePgDtInt2.get(0).getKey()));
    assertEquals(pg_dt_int2, Short.parseShort(getGanttKey(pgDtDatePgDtInt2, String.valueOf(pg_dt_int2))));

    List<GanttColumnCount> pgDtInt2PgDtInt4 = getGanttColumn(tableName, pgDtInt2, pgDtInt4);
    assertEquals(pg_dt_int2, Short.parseShort(pgDtInt2PgDtInt4.get(0).getKey()));
    assertEquals(pg_dt_int4, Integer.parseInt(getGanttKey(pgDtInt2PgDtInt4, String.valueOf(pg_dt_int4))));

    List<GanttColumnCount> pgDtInt4PgDtInt8 = getGanttColumn(tableName, pgDtInt4, pgDtInt8);
    assertEquals(pg_dt_int4, Integer.parseInt(pgDtInt4PgDtInt8.get(0).getKey()));
    assertEquals(pg_dt_int8, Long.parseLong(getGanttKey(pgDtInt4PgDtInt8, String.valueOf(pg_dt_int8))));

    List<GanttColumnCount> pgDtInt8PgDtText = getGanttColumn(tableName, pgDtInt8, pgDtText);
    assertEquals(pg_dt_int8, Long.parseLong(pgDtInt8PgDtText.get(0).getKey()));
    assertEquals(pg_dt_text, getGanttKey(pgDtInt8PgDtText, pg_dt_text));

    List<GanttColumnCount> pgDtTextPgDtTime = getGanttColumn(tableName, pgDtText, pgDtTime);
    assertEquals(pg_dt_text, pgDtTextPgDtTime.get(0).getKey());
    assertEquals(pg_dt_time_int, Integer.parseInt(getGanttKey(pgDtTextPgDtTime, String.valueOf(pg_dt_time_int))));

    List<GanttColumnCount> pgDtTimePgDtUuid = getGanttColumn(tableName, pgDtTime, pgDtUuid);
    assertEquals(pg_dt_time_int, Integer.parseInt(pgDtTimePgDtUuid.get(0).getKey()));
    assertEquals(pg_dt_uuid.toString(), getGanttKey(pgDtTimePgDtUuid, pg_dt_uuid.toString()));

    List<GanttColumnCount> pgDtUuidPgDtBytea = getGanttColumn(tableName, pgDtUuid, pgDtBytea);
    assertEquals(pg_dt_uuid.toString(), pgDtUuidPgDtBytea.get(0).getKey());
    assertEquals(new String(pg_dt_bytea, StandardCharsets.UTF_8), getGanttKey(pgDtUuidPgDtBytea, new String(pg_dt_bytea, StandardCharsets.UTF_8)));

    List<GanttColumnCount> pgDtByteaPgDtMoney = getGanttColumn(tableName, pgDtBytea, pgDtMoney);
    assertEquals(new String(pg_dt_bytea, StandardCharsets.UTF_8), pgDtByteaPgDtMoney.get(0).getKey());
    assertEquals(pg_dt_money, getBigDecimalFromString(getGanttKey(pgDtByteaPgDtMoney, getBigDecimalFromString(getStackedColumnKey(tableName, pgDtMoney)).toString())));

    List<GanttColumnCount> pgDtMoneyPgDtFloat4 = getGanttColumn(tableName, pgDtMoney, pgDtFloat4);
    assertEquals(pg_dt_money, getBigDecimalFromString(pgDtMoneyPgDtFloat4.get(0).getKey()));
    assertEquals(pg_dt_float4, Float.valueOf(getGanttKey(pgDtMoneyPgDtFloat4, getStackedColumnKey(tableName, pgDtFloat4))));

    List<GanttColumnCount> pgDtFloat4PgDtFloat8 = getGanttColumn(tableName, pgDtFloat4, pgDtFloat8);
    assertEquals(String.format("%.2f", pg_dt_float4), String.format("%.2f", Float.valueOf(pgDtFloat4PgDtFloat8.get(0).getKey())));
    assertEquals(String.format("%.2f", pg_dt_float8), String.format("%.2f", Float.valueOf(getGanttKey(pgDtFloat4PgDtFloat8, getStackedColumnKey(tableName, pgDtFloat8)))));

    List<GanttColumnCount> pgDtFloat8PgDtSerial = getGanttColumn(tableName, pgDtFloat8, pgDtSerial);
    assertEquals(String.format("%.2f", pg_dt_float8), String.format("%.2f", Float.valueOf(pgDtFloat8PgDtSerial.get(0).getKey())));
    assertEquals(pg_dt_serial, Long.parseLong(getGanttKey(pgDtFloat8PgDtSerial, getStackedColumnKey(tableName, pgDtSerial))));

    List<GanttColumnCount> pPgDtSerialPgDtSmallserial = getGanttColumn(tableName, pgDtSerial, pgDtSmallserial);
    assertEquals(pg_dt_serial, Long.parseLong((pPgDtSerialPgDtSmallserial.get(0).getKey())));
    assertEquals(pg_dt_smallserial, Long.parseLong(getGanttKey(pPgDtSerialPgDtSmallserial, getStackedColumnKey(tableName, pgDtSmallserial))));

    List<GanttColumnCount> pgDtSmallserialPgDtBigserial = getGanttColumn(tableName, pgDtSmallserial, pgDtBigserial);
    assertEquals(pg_dt_smallserial, Long.parseLong((pgDtSmallserialPgDtBigserial.get(0).getKey())));
    assertEquals(pg_dt_bigserial, Long.parseLong(getGanttKey(pgDtSmallserialPgDtBigserial, getStackedColumnKey(tableName, pgDtBigserial))));

    List<GanttColumnCount> pgDtBigserialPgDtTimetz = getGanttColumn(tableName, pgDtBigserial, pgDtTimetz);
    assertEquals(pg_dt_bigserial, Long.parseLong((pgDtBigserialPgDtTimetz.get(0).getKey())));
    assertEquals(pg_dt_timetz_int, Integer.parseInt(getGanttKey(pgDtBigserialPgDtTimetz, getStackedColumnKey(tableName, pgDtTimetz))));

    List<GanttColumnCount> pPgDtTimetzPgDtNumeric = getGanttColumn(tableName, pgDtTimetz, pgDtNumeric);
    assertEquals(pg_dt_timetz_int, Integer.parseInt((pPgDtTimetzPgDtNumeric.get(0).getKey())));
    assertEquals(pg_dt_numeric, getBigDecimalFromString(getGanttKey(pPgDtTimetzPgDtNumeric, getStackedColumnKey(tableName, pgDtNumeric))));

    List<GanttColumnCount> pgDtNumericPgDtVarchar = getGanttColumn(tableName, pgDtNumeric, pgDtVarchar);
    assertEquals(pg_dt_numeric, getBigDecimalFromString(pgDtNumericPgDtVarchar.get(0).getKey()));
    assertEquals(pg_dt_varchar, getGanttKey(pgDtNumericPgDtVarchar, getStackedColumnKey(tableName, pgDtVarchar)));

    /* Test Raw data API */
    List<List<Object>> rawDataAll = fStore.getRawDataAll(tableName, 0, Long.MAX_VALUE);
    rawDataAll.forEach(row -> cProfiles.forEach(cProfile -> {
      try {
        if (cProfile.equals(pgDtBit)) assertEquals(pg_dt_bit, getStackedColumnKey(tableName, pgDtBit));
        if (cProfile.equals(pgDtBool)) assertEquals(String.valueOf(pg_dt_bool), getStackedColumnKey(tableName, pgDtBool));
        if (cProfile.equals(pgDtChar)) assertEquals(pg_dt_char, getStackedColumnKey(tableName, pgDtChar));
        if (cProfile.equals(pgDtBpChar)) assertEquals(pg_dt_bpchar, getStackedColumnKey(tableName, pgDtBpChar));
        if (cProfile.equals(pgDtDate)) assertEquals(pg_dt_date_long, Long.parseLong(getStackedColumnKey(tableName, pgDtDate)));
        if (cProfile.equals(pgDtInt2)) assertEquals(pg_dt_int2, Short.parseShort(getStackedColumnKey(tableName, pgDtInt2)));
        if (cProfile.equals(pgDtInt4)) assertEquals(pg_dt_int4, Integer.parseInt(getStackedColumnKey(tableName, pgDtInt4)));
        if (cProfile.equals(pgDtInt8)) assertEquals(pg_dt_int8, Long.parseLong(getStackedColumnKey(tableName, pgDtInt8)));
        if (cProfile.equals(pgDtText)) assertEquals(pg_dt_text, getStackedColumnKey(tableName, pgDtText));
        if (cProfile.equals(pgDtTime)) assertEquals(pg_dt_time_int, Integer.parseInt(getStackedColumnKey(tableName, pgDtTime)));
        if (cProfile.equals(pgDtUuid)) assertEquals(pg_dt_uuid.toString(), getStackedColumnKey(tableName, pgDtUuid));
        if (cProfile.equals(pgDtBytea)) assertEquals(new String(pg_dt_bytea, StandardCharsets.UTF_8), getStackedColumnKey(tableName, pgDtBytea));
        if (cProfile.equals(pgDtMoney)) assertEquals(pg_dt_money, new BigDecimal(getStackedColumnKey(tableName, pgDtMoney)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(pgDtFloat4)) assertEquals(new BigDecimal(pg_dt_float4).setScale(2, RoundingMode.HALF_UP),
            new BigDecimal(getStackedColumnKey(tableName, pgDtFloat4)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(pgDtFloat8)) assertEquals(new BigDecimal(pg_dt_float8).setScale(2, RoundingMode.HALF_UP),
            new BigDecimal(getStackedColumnKey(tableName, pgDtFloat8)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(pgDtSerial)) assertEquals(pg_dt_serial, Long.parseLong(getStackedColumnKey(tableName, pgDtSerial)));
        if (cProfile.equals(pgDtSmallserial)) assertEquals(pg_dt_smallserial, Long.parseLong(getStackedColumnKey(tableName, pgDtSmallserial)));
        if (cProfile.equals(pgDtBigserial)) assertEquals(pg_dt_bigserial, Long.parseLong(getStackedColumnKey(tableName, pgDtBigserial)));
        if (cProfile.equals(pgDtTimetz)) assertEquals(pg_dt_timetz_int, Integer.parseInt(getStackedColumnKey(tableName, pgDtTimetz)));
        if (cProfile.equals(pgDtNumeric)) assertEquals(pg_dt_numeric, new BigDecimal(getStackedColumnKey(tableName, pgDtNumeric)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(pgDtVarchar)) assertEquals(pg_dt_varchar, getStackedColumnKey(tableName, pgDtVarchar));
        if (cProfile.equals(pgDtJsonb)) assertEquals(pg_dt_jsonb_string, getStackedColumnKey(tableName, pgDtJsonb));
        if (cProfile.equals(pgDtPoint)) assertEquals(pg_dt_point.getValue(), getStackedColumnKey(tableName, pgDtPoint));
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    }));
  }

  private BigDecimal getBigDecimalFromString(String value) {
    return new BigDecimal(value).setScale(2, RoundingMode.HALF_UP);
  }

  private String getGanttKey(List<GanttColumnCount> ganttColumnCountList, String filter) {
    return ganttColumnCountList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> f.getKey().equalsIgnoreCase(filter))
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow();
  }

  private String getStackedColumnKey(String tableName, CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getStacked(tableName, cProfile, GroupFunction.COUNT, 0, Long.MAX_VALUE)
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

  private List<GanttColumnCount> getGanttColumn(String tableName, CProfile cProfileFirst, CProfile cProfileSecond)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    return fStore.getGantt(tableName, cProfileFirst, cProfileSecond, 0, Long.MAX_VALUE);
  }
}
