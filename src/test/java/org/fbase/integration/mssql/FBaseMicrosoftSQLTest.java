package org.fbase.integration.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractMicrosoftSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
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
public class FBaseMicrosoftSQLTest extends AbstractMicrosoftSQLTest {

  private final String selectAshLike = "SELECT getdate() as dt, s.session_id, s.login_time, s.host_name, s.program_name,\n" +
          "s.login_name, s.nt_user_name, s.is_user_process,\n" +
          "s.database_id, DB_NAME(s.database_id) AS [database], \n" +
          "s.status,\n" +
          "s.reads, s.writes, s.logical_reads, s.row_count,\n" +
          "c.session_id, c.net_transport, c.protocol_type, \n" +
          "c.client_net_address, c.client_tcp_port, \n" +
          "c.num_writes AS DataPacketWrites \n" +
          "FROM sys.dm_exec_sessions s\n" +
          "INNER JOIN sys.dm_exec_connections c\n" +
          "ON s.session_id = c.session_id \n" +
          "INNER JOIN sys.dm_exec_requests r \n" +
          "ON s.session_id = r.session_id";
  private final String selectRandom = "SELECT GETDATE() as dt\n" +
          "        ,MIN(floor(rand()*(10-1+1))+1) as value_histogram\n" +
          "        ,MIN(floor(rand()*(10-1+1))+2) as value_enum\n" +
          "        ,MIN(floor(rand()*(10-1+1))+3) as value_raw \n" +
          "  FROM \n" +
          "  (SELECT id, nr, ROW_NUMBER() OVER (ORDER BY id * rand()) as rn\n" +
          "             FROM \n" +
          "             (SELECT * FROM \n" +
          "             (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id FROM sys.objects a CROSS JOIN sys.objects b) t1\n" +
          "             CROSS JOIN \n" +
          "             (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS nr FROM sys.objects a CROSS JOIN sys.objects b) t2) t3) sub\n" +
          "   GROUP BY id";

  // TODO MSSql Data Types: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
  List<String> includeList = List.of(/*"bit",*/ "int", /*"char",*/ /*"date",*/ "real", /*"text",*/ /*"time", "float", "money",*/
      /*"nchar",*/ "ntext", "bigint", "binary", "decimal", /*"numeric",*/ "sysname", "tinyint", /*"varchar",*/ "datetime",
      "nvarchar", "smallint", "datetime2", /*"timestamp",*/ "varbinary", "smallmoney", "smalldatetime", "uniqueidentifier");
  List<String> includeListAll = List.of("bit", "int", "char", "date", "real", "text", "time", "float", "money",
      "nchar", "ntext", "bigint", "binary", "decimal", "numeric", "sysname", "tinyint", "varchar", "datetime",
      "nvarchar", "smallint", "datetime2", "timestamp", "varbinary", "smallmoney", "smalldatetime", "uniqueidentifier");

  private final String selectDataType = "SELECT * FROM mssql_data_types";

  protected final String tableNameDataType = "mssql_table_dt";

  @BeforeAll
  public void initialLoading() {
    try {
      loadData(fStore, dbConnection, selectRandom, getSProfileForRandom(), log,20000, 20000);
      loadData(fStore, dbConnection, selectAshLike, getSProfileForAsh(selectAshLike), log,20000, 20000);
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
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileHistogram, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsEnum =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileEnum, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsRaw =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileRaw, GroupFunction.COUNT, 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsHistogram);
    System.out.println(stackedColumnsEnum);
    System.out.println(stackedColumnsRaw);
  }

  @Test
  public void selectAshTest()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {

    SProfile sProfile = getSProfileForAsh(selectAshLike);

    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectAshLike, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileLoginName = cProfiles.stream().filter(f -> f.getColName().equals("LOGIN_NAME")).findAny().get();
    CProfile cProfileSessId = cProfiles.stream().filter(f -> f.getColName().equals("SESSION_ID")).findAny().get();
    CProfile cProfileProgramName = cProfiles.stream().filter(f -> f.getColName().equals("PROGRAM_NAME")).findAny().get();
    CProfile cProfileIsUserProcess = cProfiles.stream().filter(f -> f.getColName().equals("IS_USER_PROCESS")).findAny().get();

    List<StackedColumn> stackedColumnsBySampleTime =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileLoginName, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsBySqlId =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSessId, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByEvent =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileProgramName, GroupFunction.COUNT, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByIsUserProcess =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileIsUserProcess, GroupFunction.COUNT, 0, Long.MAX_VALUE);

    List<List<Object>> rawData = fStore.getRawDataAll(tProfile.getTableName(), 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsBySampleTime);
    System.out.println(stackedColumnsBySqlId);
    System.out.println(stackedColumnsByEvent);
    System.out.println(stackedColumnsByIsUserProcess);

    System.out.println(rawData);
  }

  @Test
  public void loadDataTypes() throws SQLException {
    ResultSet r = dbConnection.getMetaData().getTypeInfo();

    loadDataTypes(r, includeList, 31);
  }

  @Test
  public void testDataTypes()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException, ParseException {
    String createTableDt = """
             CREATE TABLE mssql_data_types (
                   mssql_dt_bit BIT,
                   mssql_dt_int INT,
                   mssql_dt_char CHAR(20),
                   mssql_dt_date DATE,
                   mssql_dt_real REAL,
                   mssql_dt_text TEXT,
                   mssql_dt_time TIME,
                   mssql_dt_float FLOAT,
                   mssql_dt_money MONEY,
                   mssql_dt_nchar NCHAR(20),
                   mssql_dt_ntext NTEXT,
                   mssql_dt_bigint BIGINT,
                   mssql_dt_binary BINARY(30),
                   mssql_dt_decimal DECIMAL(10, 2),
                   mssql_dt_numeric NUMERIC(10, 2),
                   mssql_dt_sysname SYSNAME,
                   mssql_dt_tinyint TINYINT,
                   mssql_dt_varchar VARCHAR(20),
                   mssql_dt_datetime DATETIME,
                   mssql_dt_nvarchar NVARCHAR(20),
                   mssql_dt_smallint SMALLINT,
                   mssql_dt_datetime2 DATETIME2,
                   mssql_dt_timestamp TIMESTAMP,
                   mssql_dt_varbinary VARBINARY(30),
                   mssql_dt_smallmoney SMALLMONEY,
                   mssql_dt_smalldatetime SMALLDATETIME,
                   mssql_dt_uniqueidentifier UNIQUEIDENTIFIER
                 )
        """;

    String bitValue = "1";
    boolean bitValueTrue = true;
    int intValue = 123;
    String charValue = "Sample Char";
    java.sql.Date dateValue = java.sql.Date.valueOf("2023-10-10");
    float floatValue = 123.455f;
    String textValue = "Sample Text";
    java.sql.Time timeValue = java.sql.Time.valueOf("12:10:20");
    int timeValueInt = Math.toIntExact(timeValue.getTime());
    double doubleValue = 123.456;
    BigDecimal decimalValue = new BigDecimal("123.45");
    String ncharValue = "Sample Nchar";
    String ntextValue = "Sample Ntext";
    long bigintValue = 1234567890L;
    byte[] binaryValue = "Test bytea".getBytes(StandardCharsets.UTF_8);
    BigDecimal decimalValue1 = new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP);
    BigDecimal decimalValue2 = new BigDecimal("123.46");
    String sysnameValue = "Sample Sysname";
    short tinyintValue = (short) 23;
    String varcharValue = "Sample Varchar";
    java.sql.Timestamp datetimeValue = java.sql.Timestamp.valueOf("2023-10-10 12:10:10");
    String nvarcharValue = "Sample Nvarchar";
    short smallintValue = (short) 223;
    java.sql.Timestamp datetime2Value = java.sql.Timestamp.valueOf("2023-10-10 12:12:12");
    //java.sql.Timestamp timestampValue = new java.sql.Timestamp(new Date().getTime());
    byte[] varbinaryValue = "Test varbinaryValue".getBytes(StandardCharsets.UTF_8);
    BigDecimal smallmoneyValue = new BigDecimal("123.45");
    java.sql.Timestamp smallDatetimeValue = java.sql.Timestamp.valueOf("2023-10-10 11:11:11");
    String uniqueIdentifierValue = UUID.randomUUID().toString().toUpperCase();

    Statement createTableStmt = dbConnection.createStatement();

    String tableNameOracle = "mssql_data_types";
    dropTable(dbConnection, tableNameOracle);
    createTableStmt.executeUpdate(createTableDt);

    String insertQuery = """
         INSERT INTO mssql_data_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DEFAULT, ?, ?, ?, ?)
            """;

    try (PreparedStatement ps = dbConnection.prepareStatement(insertQuery)) {
      ps.setBoolean(1, bitValueTrue);
      ps.setInt(2, intValue);
      ps.setString(3, charValue);
      ps.setDate(4, dateValue);
      ps.setFloat(5, floatValue);
      ps.setString(6, textValue);
      ps.setTime(7, timeValue);
      ps.setDouble(8, doubleValue);
      ps.setBigDecimal(9, decimalValue);
      ps.setNString(10, ncharValue);
      ps.setString(11, ntextValue);
      ps.setLong(12, bigintValue);
      ps.setBytes(13, binaryValue);
      ps.setBigDecimal(14, decimalValue1);
      ps.setBigDecimal(15, decimalValue2);
      ps.setString(16, sysnameValue);
      ps.setShort(17, tinyintValue);
      ps.setString(18, varcharValue);
      ps.setTimestamp(19, datetimeValue);
      ps.setString(20, nvarcharValue);
      ps.setShort(21, smallintValue);
      ps.setTimestamp(22, datetime2Value);
      //statement.setTimestamp(23, timestampValue);
      ps.setBytes(23, varbinaryValue);
      ps.setBigDecimal(24, smallmoneyValue);
      ps.setTimestamp(25, smallDatetimeValue);
      ps.setString(26, uniqueIdentifierValue);

      ps.executeUpdate();
    }

    Statement selectStmt = dbConnection.createStatement();
    ResultSet resultSet = selectStmt.executeQuery(selectDataType);

    while (resultSet.next()) {
      boolean retrieved_bitValue = resultSet.getBoolean(1);
      int retrieved_intValue = resultSet.getInt(2);
      String retrieved_charValue = resultSet.getString(3);
      java.sql.Date retrieved_dateValue = resultSet.getDate(4);
      float retrieved_floatValue = resultSet.getFloat(5);
      String retrieved_textValue = resultSet.getString(6);
      java.sql.Time retrieved_timeValue = resultSet.getTime(7);
      double retrieved_doubleValue = resultSet.getDouble(8);
      BigDecimal retrieved_decimalValue = resultSet.getBigDecimal(9);
      String retrieved_ncharValue = resultSet.getNString(10);
      String retrieved_ntextValue = resultSet.getString(11);
      long retrieved_bigintValue = resultSet.getLong(12);
      byte[] retrieved_binaryValue = resultSet.getBytes(13);
      BigDecimal retrieved_decimalValue1 = resultSet.getBigDecimal(14);
      BigDecimal retrieved_decimalValue2 = resultSet.getBigDecimal(15);
      String retrieved_sysnameValue = resultSet.getString(16);
      short retrieved_tinyintValue = resultSet.getShort(17);
      String retrieved_varcharValue = resultSet.getString(18);
      java.sql.Timestamp retrieved_datetimeValue = resultSet.getTimestamp(19);
      String retrieved_nvarcharValue = resultSet.getString(20);
      short retrieved_smallintValue = resultSet.getShort(21);
      java.sql.Timestamp retrieved_datetime2Value = resultSet.getTimestamp(22);
      //java.sql.Timestamp retrieved_timestampValue = resultSet.getTimestamp(23);
      byte[] retrieved_varbinaryValue = resultSet.getBytes(24);
      BigDecimal retrieved_smallmoneyValue = resultSet.getBigDecimal(25);
      Timestamp retrieved_smallDatetimeValue = resultSet.getTimestamp(26);
      String retrieved_uniqueIdentifierValue = resultSet.getString(27);

      assertEquals(bitValue, retrieved_bitValue ? "1" : "0");
      assertEquals(intValue, retrieved_intValue);
      assertEquals(charValue, retrieved_charValue.trim());
      assertEquals(dateValue, retrieved_dateValue);
      assertEquals(floatValue, retrieved_floatValue, 0.0f); // Delta 0.0f for float comparison
      assertEquals(textValue, retrieved_textValue);
      assertEquals(timeValue, retrieved_timeValue);
      assertEquals(doubleValue, retrieved_doubleValue, 0.0); // Delta 0.0 for double comparison
      assertEquals(decimalValue, retrieved_decimalValue.setScale(2, RoundingMode.HALF_UP));
      assertEquals(ncharValue, retrieved_ncharValue.trim());
      assertEquals(ntextValue, retrieved_ntextValue);
      assertEquals(bigintValue, retrieved_bigintValue);
      assertEquals(new String(binaryValue, StandardCharsets.UTF_8), new String(retrieved_binaryValue, StandardCharsets.UTF_8).trim());
      assertEquals(decimalValue1, retrieved_decimalValue1);
      assertEquals(decimalValue2, retrieved_decimalValue2);
      assertEquals(sysnameValue, retrieved_sysnameValue);
      assertEquals(tinyintValue, retrieved_tinyintValue);
      assertEquals(varcharValue, retrieved_varcharValue);
      assertEquals(datetimeValue, retrieved_datetimeValue);
      assertEquals(nvarcharValue, retrieved_nvarcharValue);
      assertEquals(smallintValue, retrieved_smallintValue);
      assertEquals(datetime2Value, retrieved_datetime2Value);
      //assertEquals(timestampValue, retrieved_timestampValue); DEFAULT value, created on a server side
      assertEquals(new String(varbinaryValue, StandardCharsets.UTF_8), new String(retrieved_varbinaryValue, StandardCharsets.UTF_8));
      assertEquals(smallmoneyValue, retrieved_smallmoneyValue.setScale(2, RoundingMode.HALF_UP));
      assertEquals(smallDatetimeValue.toLocalDateTime().withSecond(0), retrieved_smallDatetimeValue.toLocalDateTime().withSecond(0));
      assertEquals(uniqueIdentifierValue, retrieved_uniqueIdentifierValue);
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

    CProfile mssqlDtBit = getCProfile(cProfiles, "mssql_dt_bit");
    CProfile mssqlDtInt = getCProfile(cProfiles, "mssql_dt_int");
    CProfile mssqlDtChar = getCProfile(cProfiles, "mssql_dt_char");
    CProfile mssqlDtDate = getCProfile(cProfiles, "mssql_dt_date");
    CProfile mssqlDtReal = getCProfile(cProfiles, "mssql_dt_real");
    CProfile mssqlDtText = getCProfile(cProfiles, "mssql_dt_text");
    CProfile mssqlDtTime = getCProfile(cProfiles, "mssql_dt_time");
    CProfile mssqlDtFloat = getCProfile(cProfiles, "mssql_dt_float");
    CProfile mssqlDtMoney = getCProfile(cProfiles, "mssql_dt_money");
    CProfile mssqlDtNchar = getCProfile(cProfiles, "mssql_dt_nchar");
    CProfile mssqlDtNtext = getCProfile(cProfiles, "mssql_dt_ntext");
    CProfile mssqlDtBigint = getCProfile(cProfiles, "mssql_dt_bigint");
    CProfile mssqlDtBinary = getCProfile(cProfiles, "mssql_dt_binary");
    CProfile mssqlDtDecimal = getCProfile(cProfiles, "mssql_dt_decimal");
    CProfile mssqlDtNumeric = getCProfile(cProfiles, "mssql_dt_numeric");
    CProfile mssqlDtSysname = getCProfile(cProfiles, "mssql_dt_sysname");
    CProfile mssqlDtTinyint = getCProfile(cProfiles, "mssql_dt_tinyint");
    CProfile mssqlDtVarchar = getCProfile(cProfiles, "mssql_dt_varchar");
    CProfile mssqlDtDatetime = getCProfile(cProfiles, "mssql_dt_datetime");
    CProfile mssqlDtNvarchar = getCProfile(cProfiles, "mssql_dt_nvarchar");
    CProfile mssqlDtSmallint = getCProfile(cProfiles, "mssql_dt_smallint");
    CProfile mssqlDtDatetime2 = getCProfile(cProfiles, "mssql_dt_datetime2");
    //CProfile mssqlDtTimestamp = getCProfile(cProfiles, "mssql_dt_timestamp");
    CProfile mssqlDtVarbinary = getCProfile(cProfiles, "mssql_dt_varbinary");
    CProfile mssqlDtSmallmoney = getCProfile(cProfiles, "mssql_dt_smallmoney");
    CProfile mssqlDtSmalldatetime = getCProfile(cProfiles, "mssql_dt_smalldatetime");
    CProfile mssqlDtUniqueidentifier = getCProfile(cProfiles, "mssql_dt_uniqueidentifier");

    /* Test StackedColumn API */
    assertEquals(bitValue, getStackedColumnKey(tableName, mssqlDtBit));
    assertEquals(intValue, Integer.valueOf(getStackedColumnKey(tableName, mssqlDtInt)));
    assertEquals(charValue, getStackedColumnKey(tableName, mssqlDtChar));
    assertEquals(dateValue.getTime(), Long.valueOf(getStackedColumnKey(tableName, mssqlDtDate)));
    assertEquals(floatValue, Float.valueOf(getStackedColumnKey(tableName, mssqlDtReal)));
    assertEquals(textValue, getStackedColumnKey(tableName, mssqlDtText));
    assertEquals(timeValueInt, Integer.parseInt(getStackedColumnKey(tableName, mssqlDtTime)));
    assertEquals(doubleValue, Double.valueOf(getStackedColumnKey(tableName, mssqlDtFloat)));
    assertEquals(decimalValue, new BigDecimal(getStackedColumnKey(tableName, mssqlDtMoney)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(ncharValue, getStackedColumnKey(tableName, mssqlDtNchar));
    assertEquals(ntextValue, getStackedColumnKey(tableName, mssqlDtNtext));
    assertEquals(bigintValue, Long.valueOf(getStackedColumnKey(tableName, mssqlDtBigint)));
    assertEquals(new String(binaryValue, StandardCharsets.UTF_8), getStackedColumnKey(tableName, mssqlDtBinary).trim());
    assertEquals(decimalValue1, new BigDecimal(getStackedColumnKey(tableName, mssqlDtDecimal)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(decimalValue2,  new BigDecimal(getStackedColumnKey(tableName, mssqlDtNumeric)).setScale(2, RoundingMode.HALF_UP));
    assertEquals(sysnameValue, getStackedColumnKey(tableName, mssqlDtSysname));
    assertEquals(tinyintValue, Short.valueOf(getStackedColumnKey(tableName, mssqlDtTinyint)));
    assertEquals(varcharValue, getStackedColumnKey(tableName, mssqlDtVarchar));
    //assertEquals(datetimeValue, getStackedColumnKey(tableName, mssqlDtDatetime)); // Not supported for timestamp column..
    assertEquals(nvarcharValue, getStackedColumnKey(tableName, mssqlDtNvarchar));
    assertEquals(smallintValue, Short.valueOf(getStackedColumnKey(tableName, mssqlDtSmallint)));

    DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    Date date = formatter.parse(getStackedColumnKey(tableName, mssqlDtDatetime2));
    assertEquals(datetime2Value.getTime(), date.getTime());

    assertEquals(new String(varbinaryValue, StandardCharsets.UTF_8), getStackedColumnKey(tableName, mssqlDtVarbinary));
    assertEquals(smallmoneyValue, new BigDecimal(getStackedColumnKey(tableName, mssqlDtSmallmoney)).setScale(2, RoundingMode.HALF_UP));

    Date dateSmall = formatter.parse(getStackedColumnKey(tableName, mssqlDtSmalldatetime));
    LocalDateTime localDateTimeSmall = Instant.ofEpochMilli(dateSmall.getTime())
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime();
    assertEquals(smallDatetimeValue.toLocalDateTime().withSecond(0), localDateTimeSmall.withSecond(0));
    assertEquals(uniqueIdentifierValue, getStackedColumnKey(tableName, mssqlDtUniqueidentifier));

    /* Test GanttColumn API */
    List<GanttColumn> mssqlDtBitInt = getGanttColumn(tableName, mssqlDtBit, mssqlDtInt);
    assertEquals(bitValue, mssqlDtBitInt.get(0).getKey());
    assertEquals(intValue, Integer.valueOf(getGanttKey(mssqlDtBitInt, String.valueOf(intValue))));

    List<GanttColumn> mssqlDtIntChar = getGanttColumn(tableName, mssqlDtInt, mssqlDtChar);
    assertEquals(intValue, Integer.valueOf(mssqlDtIntChar.get(0).getKey()));
    assertEquals(charValue, getGanttKey(mssqlDtIntChar, charValue));

    List<GanttColumn> mssqlDtCharDate = getGanttColumn(tableName, mssqlDtChar, mssqlDtDate);
    assertEquals(charValue, mssqlDtCharDate.get(0).getKey());
    assertEquals(dateValue.getTime(), Long.valueOf(getGanttKey(mssqlDtCharDate, String.valueOf(dateValue.getTime()))));

    List<GanttColumn> mssqlDtDateReal = getGanttColumn(tableName, mssqlDtDate, mssqlDtReal);
    assertEquals(dateValue.getTime(), Long.valueOf(mssqlDtDateReal.get(0).getKey()));
    assertEquals(floatValue, Float.valueOf(getGanttKeyFloat(mssqlDtDateReal, String.format("%.2f", floatValue))));

    List<GanttColumn> mssqlDtRealText = getGanttColumn(tableName, mssqlDtReal, mssqlDtText);
    assertEquals(floatValue, Float.valueOf(mssqlDtRealText.get(0).getKey()));
    assertEquals(textValue, getGanttKey(mssqlDtRealText, textValue));

    List<GanttColumn> mssqlDtTextTime = getGanttColumn(tableName, mssqlDtText, mssqlDtTime);
    assertEquals(textValue, mssqlDtTextTime.get(0).getKey());
    assertEquals(timeValueInt, Integer.parseInt(getGanttKey(mssqlDtTextTime, String.valueOf(timeValueInt))));

    List<GanttColumn> mssqlDtTimeFloat = getGanttColumn(tableName, mssqlDtTime, mssqlDtFloat);
    assertEquals(timeValueInt, Integer.parseInt(mssqlDtTimeFloat.get(0).getKey()));
    assertEquals(doubleValue, Double.valueOf(getGanttKey(mssqlDtTimeFloat, String.valueOf(doubleValue))));

    List<GanttColumn> mssqlDtFloatDec = getGanttColumn(tableName, mssqlDtFloat, mssqlDtDecimal);
    assertEquals(doubleValue, Double.valueOf(mssqlDtFloatDec.get(0).getKey()));
    assertEquals(decimalValue1, new BigDecimal(getGanttKey(mssqlDtFloatDec, decimalValue1.toPlainString())).setScale(2, RoundingMode.HALF_UP));

    List<GanttColumn> mssqlDtDecimalNChar = getGanttColumn(tableName, mssqlDtDecimal, mssqlDtNchar);
    assertEquals(decimalValue1, new BigDecimal(mssqlDtDecimalNChar.get(0).getKey()).setScale(2, RoundingMode.HALF_UP));
    assertEquals(ncharValue, getGanttKey(mssqlDtDecimalNChar, ncharValue));

    List<GanttColumn> mssqlDtNcharNText = getGanttColumn(tableName, mssqlDtNchar, mssqlDtNtext);
    assertEquals(ncharValue, mssqlDtNcharNText.get(0).getKey());
    assertEquals(ntextValue, getGanttKey(mssqlDtNcharNText, ntextValue));

    List<GanttColumn> mssqlDtNtextBigInt = getGanttColumn(tableName, mssqlDtNtext, mssqlDtBigint);
    assertEquals(ntextValue, mssqlDtNtextBigInt.get(0).getKey());
    assertEquals(bigintValue, Long.valueOf(getGanttKey(mssqlDtNtextBigInt, String.valueOf(bigintValue))));

    List<GanttColumn> mssqlDtBigintBinary = getGanttColumn(tableName, mssqlDtBigint, mssqlDtBinary);
    assertEquals(bigintValue, Long.valueOf(mssqlDtBigintBinary.get(0).getKey()));
    assertEquals(new String(binaryValue, StandardCharsets.UTF_8).trim(), getGanttKey(mssqlDtBigintBinary, new String(binaryValue, StandardCharsets.UTF_8).trim()).trim());

    /* Test Raw data API */
    List<List<Object>> rawDataAll = fStore.getRawDataAll(tableName, 0, Long.MAX_VALUE);

    rawDataAll.forEach(row -> cProfiles.forEach(cProfile -> {
      try {
        if (cProfile.equals(mssqlDtBit)) assertEquals(bitValue, getStackedColumnKey(tableName, mssqlDtBit));
        if (cProfile.equals(mssqlDtInt)) assertEquals(intValue, Integer.valueOf(getStackedColumnKey(tableName, mssqlDtInt)));
        if (cProfile.equals(mssqlDtChar)) assertEquals(charValue, getStackedColumnKey(tableName, mssqlDtChar));
        if (cProfile.equals(mssqlDtDate)) assertEquals(dateValue.getTime(), Long.valueOf(getStackedColumnKey(tableName, mssqlDtDate)));
        if (cProfile.equals(mssqlDtReal)) assertEquals(floatValue, Float.valueOf(getStackedColumnKey(tableName, mssqlDtReal)));
        if (cProfile.equals(mssqlDtText)) assertEquals(textValue, getStackedColumnKey(tableName, mssqlDtText));
        if (cProfile.equals(mssqlDtTime)) assertEquals(timeValueInt, Integer.parseInt(getStackedColumnKey(tableName, mssqlDtTime)));
        if (cProfile.equals(mssqlDtFloat)) assertEquals(doubleValue, Double.valueOf(getStackedColumnKey(tableName, mssqlDtFloat)));
        if (cProfile.equals(mssqlDtMoney)) assertEquals(decimalValue, new BigDecimal(getStackedColumnKey(tableName, mssqlDtMoney)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(mssqlDtNchar)) assertEquals(ncharValue, getStackedColumnKey(tableName, mssqlDtNchar));
        if (cProfile.equals(mssqlDtNtext)) assertEquals(ntextValue, getStackedColumnKey(tableName, mssqlDtNtext));
        if (cProfile.equals(mssqlDtBigint)) assertEquals(bigintValue, Long.valueOf(getStackedColumnKey(tableName, mssqlDtBigint)));
        if (cProfile.equals(mssqlDtBinary)) assertEquals(new String(binaryValue, StandardCharsets.UTF_8).trim(), getStackedColumnKey(tableName, mssqlDtBinary).trim());
        if (cProfile.equals(mssqlDtDecimal)) assertEquals(decimalValue1, new BigDecimal(getStackedColumnKey(tableName, mssqlDtDecimal)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(mssqlDtNumeric)) assertEquals(decimalValue2, new BigDecimal(getStackedColumnKey(tableName, mssqlDtNumeric)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(mssqlDtSysname)) assertEquals(sysnameValue, getStackedColumnKey(tableName, mssqlDtSysname));
        if (cProfile.equals(mssqlDtTinyint)) assertEquals(tinyintValue, Short.valueOf(getStackedColumnKey(tableName, mssqlDtTinyint)));
        if (cProfile.equals(mssqlDtVarchar)) assertEquals(varcharValue, getStackedColumnKey(tableName, mssqlDtVarchar));
        if (cProfile.equals(mssqlDtNvarchar)) assertEquals(nvarcharValue, getStackedColumnKey(tableName, mssqlDtNvarchar));
        if (cProfile.equals(mssqlDtSmallint)) assertEquals(smallintValue, Short.valueOf(getStackedColumnKey(tableName, mssqlDtSmallint)));

        Date datetime2 = formatter.parse(getStackedColumnKey(tableName, mssqlDtDatetime2));
        if (cProfile.equals(mssqlDtDatetime2)) assertEquals(datetime2Value.getTime(), datetime2.getTime());
        if (cProfile.equals(mssqlDtVarbinary)) assertEquals(new String(varbinaryValue, StandardCharsets.UTF_8), getStackedColumnKey(tableName, mssqlDtVarbinary));
        if (cProfile.equals(mssqlDtSmallmoney)) assertEquals(smallmoneyValue, new BigDecimal(getStackedColumnKey(tableName, mssqlDtSmallmoney)).setScale(2, RoundingMode.HALF_UP));
        if (cProfile.equals(mssqlDtSmalldatetime)) assertEquals(smallDatetimeValue.toLocalDateTime().withSecond(0), localDateTimeSmall.withSecond(0));
        if (cProfile.equals(mssqlDtUniqueidentifier)) assertEquals(uniqueIdentifierValue, getStackedColumnKey(tableName, mssqlDtUniqueidentifier));
      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    }));
  }

  protected SProfile getSProfile(String select) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, dbConnection).getCsTypeMap().forEach((key, value) -> {
      if (key.equalsIgnoreCase("mssql_dt_datetime")) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else if (key.equalsIgnoreCase("mssql_dt_varbinary")) {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.HISTOGRAM).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.RAW).build());
      }
    });

    return new SProfile().setTableName(tableNameDataType)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.GLOBAL)
        .setBackendType(BType.BERKLEYDB)
        .setCompression(false)
        .setCsTypeMap(csTypeMap);
  }

  private static void dropTable(Connection connection, String tableName) throws SQLException {
    String sql = "DROP TABLE IF EXISTS " + tableName;

    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  private String getGanttKey(List<GanttColumn> ganttColumnList, String filter) {
    return ganttColumnList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> f.getKey().trim().equalsIgnoreCase(filter))
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private String getGanttKeyFloat(List<GanttColumn> ganttColumnList, String filter) {
    return ganttColumnList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> {
              Float val = Float.valueOf(f.getKey());
              String valStr = String.format("%.2f", val);
              return valStr.equals(filter);
        })
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private List<GanttColumn> getGanttColumn(String tableName, CProfile cProfileFirst, CProfile cProfileSecond)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    return fStore.getGColumnListTwoLevelGroupBy(tableName, cProfileFirst, cProfileSecond, 0, Long.MAX_VALUE);
  }

  private String getStackedColumnKey(String tableName, CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tableName, cProfile, GroupFunction.COUNT, 0, Long.MAX_VALUE)
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

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow();
  }
}
