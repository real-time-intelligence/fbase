package org.fbase.integration.ch;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractClickhouseSQLTest;
import org.fbase.common.AbstractMicrosoftSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseClickHouseSQLTest extends AbstractClickhouseSQLTest {

  // TODO MSSql Data Types: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
  List<String> includeList = List.of(/*"bit",*/ "dec", /*"decimal", "numeric",*/
      "byte", /*"bool",*/ "boolean", /*"char", "clob",*/ "fixedstring",
      /*"date",*//* "time",*/ "date32",/* "datetime",*/ /*"timestamp",*/
      "enum", "enum8", "enum16",
      /*"int",*/ /*"smallint",*//* "integer",*/ "int1", /*"int8", */"int16", "int32", "int64", "int128", "int256", /*"real",*/ /*"double",*/
      "uint32", "uint8", "uint16", "uint64", "float32", "float64",
     /* "text", *//*"uuid",*/  "String", /* "varchar"*/ /*"nvarchar"*/"nullable");
  List<String> includeListAll = List.of("bit", "dec", "decimal", "numeric",
      "byte", "bool", "boolean", "char", "clob", "fixedstring",
      "date", "time", "date32", "datetime", "timestamp",
      "enum", "enum8", "enum16",
      "int", "smallint", "integer", "int1", "int8", "int16", "int32", "int64", "int128", "int256", "real", "double",
      "uint32", "uint8", "uint16", "uint64", "float32", "float64",
      "text", "uuid",  "String", "varchar", "nvarchar", "nullable");

  private final String selectDataType = "SELECT * FROM default.ch_data_types";

  protected final String tableNameDataType = "ch_table_dt";

  @Test
  public void loadDataTypes() throws SQLException {
    ResultSet r = dbConnection.getMetaData().getTypeInfo();

    loadDataTypes(r, includeListAll, 47);
  }

  @Test
  public void testDataTypes()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException, ParseException {
    String createTableDt = """
             CREATE TABLE default.ch_data_types (
                    ch_dt_bit BIT(47),
                    ch_dt_dec Decimal(10, 2),
                    ch_dt_int Int32,
                    ch_dt_byte Int8,
                    ch_dt_bool Boolean,
                    ch_dt_char FixedString(1),
                    ch_dt_clob String,
                    ch_dt_date Date,
                    ch_dt_enum8 Enum8('value1' = 1, 'value2' = 2),
                    ch_dt_enum16 Enum16('valueA' = 1, 'valueB' = 2),
                    ch_dt_int1 Int1,
                    ch_dt_int8 Int8,
                    ch_dt_real Float32,
                    ch_dt_text String,
                    ch_dt_time DateTime('Europe/Moscow'),
                    ch_dt_uuid UUID,
                    ch_dt_uint8 UInt8,
                    ch_dt_enum8_2 Enum8('enumA' = 1, 'enumB' = 2),
                    ch_dt_int16 Int16,
                    ch_dt_INT32 Int32,
                    ch_dt_INT64 Int64,
                    ch_dt_double Float64,
                    ch_dt_date32 Date,
                    ch_dt_enum16_2 Enum16('enumX' = 1, 'enumY' = 2),
                    ch_dt_int128 Int128,
                    ch_dt_int256 Int256,
                    ch_dt_uint16 UInt16,
                    ch_dt_uint32 UInt32,
                    ch_dt_decimal Decimal(10,2),
                    ch_dt_float32 Float32,
                    ch_dt_float64 Float64,
                    ch_dt_integer Int32,
                    ch_dt_numeric Decimal(10,2),
                    ch_dt_varchar String,
                    ch_dt_boolean Boolean,
                    ch_dt_datetime DateTime,
                    ch_dt_nvarchar Nullable(String),
                    ch_dt_smallint Int16,
                    ch_dt_timestamp DateTime('Europe/Moscow'),
                    ch_dt_fixedstring FixedString(10)
                  ) ENGINE = MergeTree() ORDER BY (ch_dt_int)
        """;

    int ch_dt_bit = 0;
    BigDecimal ch_dt_dec = new BigDecimal("1234.56");
    int ch_dt_int = 6789;
    byte ch_dt_byte = 12;
    boolean ch_dt_bool = true;
    String ch_dt_char = "A";
    String ch_dt_clob = "Lorem ipsum dolor sit amet";
    LocalDate ch_dt_date = LocalDate.of(2023, 10, 13);
    String ch_dt_enum8 = "value1";
    String ch_dt_enum16 = "valueA";
    byte ch_dt_int1 = -5;
    byte ch_dt_int8 = -128;
    float ch_dt_real = 3.14f;
    String ch_dt_text = "Hello, world!";
    LocalDateTime ch_dt_time = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);;//LocalDateTime.of(2023, 10, 13, 16, 5, 20);
    ZonedDateTime ldtZoned = ch_dt_time.atZone(ZoneId.systemDefault());
    ZonedDateTime utcZoned = ldtZoned.withZoneSameInstant(ZoneOffset.UTC);
    ZonedDateTime mskZoned = ldtZoned.withZoneSameInstant(ZoneId.of("Europe/Moscow"));
    UUID ch_dt_uuid = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
    String ch_dt_enum8_2 = "enumA";
    short ch_dt_uint8 = 255;
    short ch_dt_int16 = 32767;
    long ch_dt_int32 = 21474836L;
    long ch_dt_int64 = 92233720L;
    double ch_dt_double = 3.14159;
    LocalDate ch_dt_date32 = LocalDate.of(2023, 10, 13);
    String ch_dt_enum16_2 = "enumX";
    String ch_dt_int128 = "123456789";
    String ch_dt_int256 = "123456789012";
    int ch_dt_uint16 = 65535;
    long ch_dt_uint32 = 4294967295L;
    BigDecimal ch_dt_decimal = new BigDecimal("234.23");
    float ch_dt_float32 = 1.2345f;
    double ch_dt_float64 = 1.23456789;
    int ch_dt_integer = -98765;
    BigDecimal ch_dt_numeric = new BigDecimal("2344.37");
    String ch_dt_varchar = "ClickHouse";
    boolean ch_dt_boolean = true;
    LocalDateTime ch_dt_datetime = LocalDateTime.of(2023, 10, 13, 16, 5, 20);
    ZonedDateTime ch_dt_datetime_zoned = ch_dt_datetime.atZone(ZoneId.systemDefault());
    ZonedDateTime ch_dt_datetime_zoned_utc = ch_dt_datetime_zoned.withZoneSameInstant(ZoneOffset.UTC);
    String ch_dt_nvarchar = "AФ";
    short ch_dt_smallint = 32767;
    LocalDateTime ch_dt_timestamp = LocalDateTime.of(2023, 10, 13, 16, 5, 20);
    ZonedDateTime ch_dt_timestamp_zoned = ch_dt_timestamp.atZone(ZoneId.systemDefault());
    ZonedDateTime ch_dt_timestamp_zoned_utc = ch_dt_timestamp_zoned.withZoneSameInstant(ZoneOffset.UTC);
    String ch_dt_fixedstring = "Hello";

    Statement createTableStmt = dbConnection.createStatement();

    String tableNameOracle = "ch_data_types";
    dropTable(dbConnection, tableNameOracle);
    createTableStmt.executeUpdate(createTableDt);

    String insertQuery = """
      INSERT INTO default.ch_data_types 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              """;

    PreparedStatement insertStatement = dbConnection.prepareStatement(insertQuery);

    insertStatement.setInt(1, ch_dt_bit);
    insertStatement.setBigDecimal(2, ch_dt_dec);
    insertStatement.setInt(3, ch_dt_int);
    insertStatement.setByte(4, ch_dt_byte);
    insertStatement.setBoolean(5, ch_dt_bool);
    insertStatement.setString(6, ch_dt_char);
    insertStatement.setString(7, ch_dt_clob);
    insertStatement.setDate(8, java.sql.Date.valueOf(ch_dt_date));
    insertStatement.setString(9, ch_dt_enum8);
    insertStatement.setString(10, ch_dt_enum16);
    insertStatement.setByte(11, ch_dt_int1);
    insertStatement.setByte(12, ch_dt_int8);
    insertStatement.setFloat(13, ch_dt_real);
    insertStatement.setString(14, ch_dt_text);
    insertStatement.setTimestamp(15, Timestamp.valueOf(ch_dt_time));
    insertStatement.setObject(16, ch_dt_uuid);
    insertStatement.setInt(17, ch_dt_uint8);
    insertStatement.setString(18, ch_dt_enum8_2);
    insertStatement.setShort(19, ch_dt_int16);
    insertStatement.setLong(20, ch_dt_int32);
    insertStatement.setLong(21, ch_dt_int64);
    insertStatement.setDouble(22, ch_dt_double);
    insertStatement.setDate(23, java.sql.Date.valueOf(ch_dt_date32));
    insertStatement.setString(24, ch_dt_enum16_2);
    insertStatement.setObject(25, ch_dt_int128);
    insertStatement.setObject(26, ch_dt_int256);
    insertStatement.setInt(27, ch_dt_uint16);
    insertStatement.setLong(28, ch_dt_uint32);
    insertStatement.setBigDecimal(29, ch_dt_decimal);
    insertStatement.setFloat(30, ch_dt_float32);
    insertStatement.setDouble(31, ch_dt_float64);
    insertStatement.setInt(32, ch_dt_integer);
    insertStatement.setBigDecimal(33, ch_dt_numeric);
    insertStatement.setString(34, ch_dt_varchar);
    insertStatement.setBoolean(35, ch_dt_boolean);
    insertStatement.setTimestamp(36, java.sql.Timestamp.valueOf(ch_dt_datetime));
    insertStatement.setString(37, ch_dt_nvarchar);
    insertStatement.setShort(38, ch_dt_smallint);
    insertStatement.setTimestamp(39, java.sql.Timestamp.valueOf(ch_dt_timestamp));
    insertStatement.setString(40, ch_dt_fixedstring);

    insertStatement.executeUpdate();

    Statement selectStmt = dbConnection.createStatement();
    ResultSet resultSet = selectStmt.executeQuery(selectDataType);

    while (resultSet.next()) {
      try {
        int retrieved_ch_dt_bit = resultSet.getInt("ch_dt_bit");
        Assertions.assertEquals(ch_dt_bit, retrieved_ch_dt_bit);

        BigDecimal retrieved_ch_dt_dec = resultSet.getBigDecimal("ch_dt_dec");
        Assertions.assertEquals(ch_dt_dec, retrieved_ch_dt_dec);

        int retrieved_ch_dt_int = resultSet.getInt("ch_dt_int");
        Assertions.assertEquals(ch_dt_int, retrieved_ch_dt_int);

        byte retrieved_ch_dt_byte = resultSet.getByte("ch_dt_byte");
        Assertions.assertEquals(ch_dt_byte, retrieved_ch_dt_byte);

        boolean retrieved_ch_dt_bool = resultSet.getBoolean("ch_dt_bool");
        Assertions.assertEquals(ch_dt_bool, retrieved_ch_dt_bool);

        String retrieved_ch_dt_char = resultSet.getString("ch_dt_char");
        Assertions.assertEquals(ch_dt_char, retrieved_ch_dt_char);

        String retrieved_ch_dt_clob = resultSet.getString("ch_dt_clob");
        Assertions.assertEquals(ch_dt_clob, retrieved_ch_dt_clob);

        LocalDate retrieved_ch_dt_date = resultSet.getDate("ch_dt_date").toLocalDate();
        Assertions.assertEquals(ch_dt_date, retrieved_ch_dt_date);

        String retrieved_ch_dt_enum8 = resultSet.getString("ch_dt_enum8");
        Assertions.assertEquals(ch_dt_enum8, retrieved_ch_dt_enum8);

        String retrieved_ch_dt_enum16 = resultSet.getString("ch_dt_enum16");
        Assertions.assertEquals(ch_dt_enum16, retrieved_ch_dt_enum16);

        byte retrieved_ch_dt_int1 = resultSet.getByte("ch_dt_int1");
        Assertions.assertEquals(ch_dt_int1, retrieved_ch_dt_int1);

        byte retrieved_ch_dt_int8 = resultSet.getByte("ch_dt_int8");
        Assertions.assertEquals(ch_dt_int8, retrieved_ch_dt_int8);

        float retrieved_ch_dt_real = resultSet.getFloat("ch_dt_real");
        Assertions.assertEquals(ch_dt_real, retrieved_ch_dt_real);

        /* TODO Approve with CSV
        String retrieved_ch_dt_text = resultSet.getString("ch_dt_text");
        Assertions.assertEquals(ch_dt_text, retrieved_ch_dt_text);*/

        Timestamp retrieved_ch_dt_time = resultSet.getTimestamp("ch_dt_time");
        Assertions.assertEquals(Timestamp.valueOf(utcZoned.toLocalDateTime()), retrieved_ch_dt_time);

        UUID retrieved_ch_dt_uuid = UUID.fromString(resultSet.getString("ch_dt_uuid"));
        Assertions.assertEquals(ch_dt_uuid, retrieved_ch_dt_uuid);

        String retrieved_ch_dt_enum8_2 = resultSet.getString("ch_dt_enum8_2");
        Assertions.assertEquals(ch_dt_enum8_2, retrieved_ch_dt_enum8_2);

        short retrieved_ch_dt_int16 = resultSet.getShort("ch_dt_int16");
        Assertions.assertEquals(ch_dt_int16, retrieved_ch_dt_int16);

        long retrieved_ch_dt_int32 = resultSet.getLong("ch_dt_int32");
        Assertions.assertEquals(ch_dt_int32, retrieved_ch_dt_int32);

        double retrieved_ch_dt_double = resultSet.getDouble("ch_dt_double");
        Assertions.assertEquals(ch_dt_double, retrieved_ch_dt_double);

        LocalDate retrieved_ch_dt_date32 = resultSet.getDate("ch_dt_date32").toLocalDate();
        Assertions.assertEquals(ch_dt_date32, retrieved_ch_dt_date32);

        String retrieved_ch_dt_enum16_2 = resultSet.getString("ch_dt_enum16_2");
        Assertions.assertEquals(ch_dt_enum16_2, retrieved_ch_dt_enum16_2);

        int retrieved_ch_dt_uint16 = resultSet.getInt("ch_dt_uint16");
        Assertions.assertEquals(ch_dt_uint16, retrieved_ch_dt_uint16);

        long retrieved_ch_dt_uint32 = resultSet.getLong("ch_dt_uint32");
        Assertions.assertEquals(ch_dt_uint32, retrieved_ch_dt_uint32);

        BigDecimal retrieved_ch_dt_decimal = resultSet.getBigDecimal("ch_dt_decimal");
        Assertions.assertEquals(ch_dt_decimal, retrieved_ch_dt_decimal);

        float retrieved_ch_dt_float32 = resultSet.getFloat("ch_dt_float32");
        Assertions.assertEquals(ch_dt_float32, retrieved_ch_dt_float32);

        double retrieved_ch_dt_float64 = resultSet.getDouble("ch_dt_float64");
        Assertions.assertEquals(new BigDecimal(ch_dt_float64).setScale(2, RoundingMode.HALF_UP), new BigDecimal(retrieved_ch_dt_float64).setScale(2, RoundingMode.HALF_UP));

        int retrieved_ch_dt_integer = resultSet.getInt("ch_dt_integer");
        Assertions.assertEquals(ch_dt_integer, retrieved_ch_dt_integer);

        BigDecimal retrieved_ch_dt_numeric = resultSet.getBigDecimal("ch_dt_numeric");
        Assertions.assertEquals(ch_dt_numeric, retrieved_ch_dt_numeric);

        String retrieved_ch_dt_varchar = resultSet.getString("ch_dt_varchar");
        Assertions.assertEquals(ch_dt_varchar, retrieved_ch_dt_varchar);

        boolean retrieved_ch_dt_boolean = resultSet.getBoolean("ch_dt_boolean");
        Assertions.assertEquals(ch_dt_boolean, retrieved_ch_dt_boolean);

        Timestamp retrieved_ch_dt_datetime = resultSet.getTimestamp("ch_dt_datetime");
        Assertions.assertEquals(Timestamp.valueOf(ch_dt_datetime_zoned_utc.toLocalDateTime()), retrieved_ch_dt_datetime);

        String retrieved_ch_dt_nvarchar = resultSet.getString("ch_dt_nvarchar");
        Assertions.assertEquals(ch_dt_nvarchar, retrieved_ch_dt_nvarchar);

        short retrieved_ch_dt_smallint = resultSet.getShort("ch_dt_smallint");
        Assertions.assertEquals(ch_dt_smallint, retrieved_ch_dt_smallint);

        Timestamp retrieved_ch_dt_timestamp = resultSet.getTimestamp("ch_dt_timestamp");
        Assertions.assertEquals(Timestamp.valueOf(ch_dt_timestamp_zoned_utc.toLocalDateTime()), retrieved_ch_dt_timestamp);

        String retrieved_ch_dt_fixedstring = resultSet.getString("ch_dt_fixedstring");
        Assertions.assertEquals(ch_dt_fixedstring, retrieved_ch_dt_fixedstring.trim());
      } catch (SQLException e) {
        log.error(e);
      }
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

    DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    /* Test StackedColumn API */
    CProfile chDtBit = getCProfile(cProfiles, "ch_dt_bit");
    assertEquals(ch_dt_bit, Integer.valueOf(getStackedColumnKey(tableName, chDtBit)));

    CProfile chDtDec = getCProfile(cProfiles, "ch_dt_dec");
    assertEquals(ch_dt_dec, new BigDecimal(getStackedColumnKey(tableName, chDtDec)).setScale(2, RoundingMode.HALF_UP));

    CProfile chDtInt = getCProfile(cProfiles, "ch_dt_int");
    assertEquals(ch_dt_int, Integer.valueOf(getStackedColumnKey(tableName, chDtInt)));

    CProfile chDtByte = getCProfile(cProfiles, "ch_dt_byte");
    assertEquals(ch_dt_byte, Byte.valueOf(getStackedColumnKey(tableName, chDtByte)));

    CProfile chDtBool = getCProfile(cProfiles, "ch_dt_bool");
    assertEquals(ch_dt_bool, Boolean.valueOf(getStackedColumnKey(tableName, chDtBool)));

    CProfile chDtChar = getCProfile(cProfiles, "ch_dt_char");
    assertEquals(ch_dt_char, getStackedColumnKey(tableName, chDtChar));

    /* TODO Approve with CSV
    CProfile chDtClob = getCProfile(cProfiles, "ch_dt_clob");
    assertEquals(ch_dt_clob, getStackedColumnKey(tableName, chDtClob));*/

    CProfile chDtDate = getCProfile(cProfiles, "ch_dt_date");
    LocalDate ch_dt_date_ld = Instant.ofEpochMilli(Long.parseLong(getStackedColumnKey(tableName, chDtDate)) * 1000).atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(ch_dt_date, ch_dt_date_ld);

    CProfile chDtEnum8 = getCProfile(cProfiles, "ch_dt_enum8");
    assertEquals(ch_dt_enum8, getStackedColumnKey(tableName, chDtEnum8));

    CProfile chDtEnum16 = getCProfile(cProfiles, "ch_dt_enum16");
    assertEquals(ch_dt_enum16, getStackedColumnKey(tableName, chDtEnum16));

    CProfile chDtInt1 = getCProfile(cProfiles, "ch_dt_int1");
    assertEquals(ch_dt_int1, Byte.valueOf(getStackedColumnKey(tableName, chDtInt1)));

    CProfile chDtInt8 = getCProfile(cProfiles, "ch_dt_int8");
    assertEquals(ch_dt_int8, Byte.valueOf(getStackedColumnKey(tableName, chDtInt8)));

    CProfile chDtReal = getCProfile(cProfiles, "ch_dt_real");
    assertEquals(ch_dt_real, Float.parseFloat(getStackedColumnKey(tableName, chDtReal)));

    /* TODO Approve with CSV
    CProfile chDtText = getCProfile(cProfiles, "ch_dt_text");
    assertEquals(ch_dt_text, getStackedColumnKey(tableName, chDtText));*/

    CProfile chDtTime = getCProfile(cProfiles, "ch_dt_time");
    Date dateTime = formatter.parse(getStackedColumnKey(tableName, chDtTime));
    LocalDateTime localDateTimeTime = new java.sql.Timestamp(dateTime.getTime()).toLocalDateTime();
    assertEquals(ch_dt_time.minusHours(3), localDateTimeTime); //TODO Investigate it

    CProfile chDtUuid = getCProfile(cProfiles, "ch_dt_uuid");
    assertEquals(ch_dt_uuid.toString(), getStackedColumnKey(tableName, chDtUuid));

    CProfile chDtUint8 = getCProfile(cProfiles, "ch_dt_uint8");
    assertEquals(ch_dt_uint8, Short.valueOf(getStackedColumnKey(tableName, chDtUint8)));

    CProfile chDtEnum8_2 = getCProfile(cProfiles, "ch_dt_enum8_2");
    assertEquals(ch_dt_enum8_2, getStackedColumnKey(tableName, chDtEnum8_2));

    CProfile chDtInt16 = getCProfile(cProfiles, "ch_dt_int16");
    assertEquals(ch_dt_int16, Short.valueOf(getStackedColumnKey(tableName, chDtInt16)));

    CProfile chDtInt32 = getCProfile(cProfiles, "ch_dt_int32");
    assertEquals(ch_dt_int32, Long.valueOf(getStackedColumnKey(tableName, chDtInt32)));

    CProfile chDtINT64 = getCProfile(cProfiles, "ch_dt_int64");
    assertEquals(ch_dt_int64, Long.valueOf(getStackedColumnKey(tableName, chDtINT64)));

    CProfile chDtDouble = getCProfile(cProfiles, "ch_dt_double");
    assertEquals(ch_dt_double, Double.valueOf(getStackedColumnKey(tableName, chDtDouble)));

    CProfile chDtDate32 = getCProfile(cProfiles, "ch_dt_date32");
    LocalDate ch_dt_date_32 = Instant.ofEpochMilli(Long.parseLong(getStackedColumnKey(tableName, chDtDate32)) * 1000).atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(ch_dt_date32, ch_dt_date_32);

    CProfile chDtEnum16_2 = getCProfile(cProfiles, "ch_dt_enum16_2");
    assertEquals(ch_dt_enum16_2, getStackedColumnKey(tableName, chDtEnum16_2));

    CProfile chDtInt128 = getCProfile(cProfiles, "ch_dt_int128");
    assertEquals(ch_dt_int128, getStackedColumnKey(tableName, chDtInt128));

    CProfile chDtInt256 = getCProfile(cProfiles, "ch_dt_int256");
    assertEquals(ch_dt_int256, getStackedColumnKey(tableName, chDtInt256));

    CProfile chDtUint16 = getCProfile(cProfiles, "ch_dt_uint16");
    assertEquals(ch_dt_uint16, Integer.valueOf(getStackedColumnKey(tableName, chDtUint16)));

    CProfile chDtUint32 = getCProfile(cProfiles, "ch_dt_uint32");
    assertEquals(ch_dt_uint32, Long.valueOf(getStackedColumnKey(tableName, chDtUint32)));

    CProfile chDtDecimal = getCProfile(cProfiles, "ch_dt_decimal");
    assertEquals(ch_dt_decimal, new BigDecimal(getStackedColumnKey(tableName, chDtDecimal)).setScale(2, RoundingMode.HALF_UP));

    CProfile chDtFloat32 = getCProfile(cProfiles, "ch_dt_float32");
    assertEquals(ch_dt_float32, Float.valueOf(getStackedColumnKey(tableName, chDtFloat32)));

    CProfile chDtFloat64 = getCProfile(cProfiles, "ch_dt_float64");
    assertEquals(String.format("%.2f", ch_dt_float64), String.format("%.2f", Optional.of(Float.valueOf(getStackedColumnKey(tableName, chDtFloat64))).get().floatValue()));

    CProfile chDtInteger = getCProfile(cProfiles, "ch_dt_integer");
    assertEquals(ch_dt_integer, Integer.valueOf(getStackedColumnKey(tableName, chDtInteger)));

    CProfile chDtNumeric = getCProfile(cProfiles, "ch_dt_numeric");
    assertEquals(ch_dt_numeric, new BigDecimal(getStackedColumnKey(tableName, chDtNumeric)).setScale(2, RoundingMode.HALF_UP));

    /* TODO Approve with CSV
    CProfile chDtVarchar = getCProfile(cProfiles, "ch_dt_varchar");
    assertEquals(ch_dt_varchar, getStackedColumnKey(tableName, chDtVarchar));*/

    CProfile chDtBoolean = getCProfile(cProfiles, "ch_dt_boolean");
    assertEquals(ch_dt_boolean, Boolean.valueOf(getStackedColumnKey(tableName, chDtBoolean)));

    CProfile chDtDatetime = getCProfile(cProfiles, "ch_dt_datetime");
    Date dateTime2 = formatter.parse(getStackedColumnKey(tableName, chDtDatetime));
    LocalDateTime localDateTimeTime2 = new java.sql.Timestamp(dateTime2.getTime()).toLocalDateTime();
    assertEquals(ch_dt_datetime, localDateTimeTime2);

    CProfile chDtNvarchar = getCProfile(cProfiles, "ch_dt_nvarchar");
    assertEquals(ch_dt_nvarchar, getStackedColumnKey(tableName, chDtNvarchar));

    CProfile chDtSmallint = getCProfile(cProfiles, "ch_dt_smallint");
    assertEquals(ch_dt_smallint, Short.valueOf(getStackedColumnKey(tableName, chDtSmallint)));

    // CProfile chDtTimestamp = getCProfile(cProfiles, "ch_dt_timestamp");
    // assertEquals(ch_dt_timestamp, getStackedColumnKey(tableName, chDtTimestamp)); //Not supported for timestamp column..

    CProfile chDtFixedstring = getCProfile(cProfiles, "ch_dt_fixedstring");
    assertEquals(ch_dt_fixedstring, getStackedColumnKey(tableName, chDtFixedstring).trim());

    /* Test GanttColumn API */
    List<GanttColumn> chDtDecInt = getGanttColumn(tableName, chDtDec, chDtInt);
    assertEquals(ch_dt_dec, new BigDecimal(chDtDecInt.get(0).getKey()).setScale(2, RoundingMode.HALF_UP));
    assertEquals(ch_dt_int, Integer.valueOf(getGanttKey(chDtDecInt, String.valueOf(ch_dt_int))));

    List<GanttColumn> chDtIntByte = getGanttColumn(tableName, chDtInt, chDtByte);
    assertEquals(ch_dt_int, Integer.valueOf(chDtIntByte.get(0).getKey()));
    assertEquals(ch_dt_byte, Byte.valueOf(getGanttKey(chDtIntByte, String.valueOf(ch_dt_byte))));

    List<GanttColumn> chDtByteBool = getGanttColumn(tableName, chDtByte, chDtBool);
    assertEquals(ch_dt_byte, Byte.valueOf(chDtByteBool.get(0).getKey()));
    assertEquals(ch_dt_bool, Boolean.valueOf(getGanttKey(chDtByteBool, String.valueOf(ch_dt_bool))));

    List<GanttColumn> chDtBoolChar = getGanttColumn(tableName, chDtBool, chDtChar);
    assertEquals(ch_dt_bool, Boolean.valueOf(chDtBoolChar.get(0).getKey()));
    assertEquals(ch_dt_char, getGanttKey(chDtBoolChar, ch_dt_char));

    /* TODO Approve with CSV
    List<GanttColumn> chDtCharClob = getGanttColumn(tableName, chDtChar, chDtClob); chDtClob
    assertEquals(ch_dt_char, chDtCharClob.get(0).getKey());
    assertEquals(ch_dt_clob, getGanttKey(chDtCharClob, ch_dt_clob));

    List<GanttColumn> chDtClobDate = getGanttColumn(tableName, chDtClob, chDtDate);
    assertEquals(ch_dt_clob, chDtClobDate.get(0).getKey());
    assertEquals(ch_dt_date, Instant.ofEpochMilli(Long.parseLong(getGanttKey(chDtClobDate, String.valueOf(Long.parseLong(getStackedColumnKey(tableName, chDtDate))))) * 1000).atZone(ZoneId.systemDefault()).toLocalDate());*/

    List<GanttColumn> chDtDateEnum8 = getGanttColumn(tableName, chDtDate, chDtEnum8);
    assertEquals(ch_dt_date, Instant.ofEpochMilli(Long.parseLong(String.valueOf(Long.parseLong(chDtDateEnum8.get(0).getKey()))) * 1000).atZone(ZoneId.systemDefault()).toLocalDate());
    assertEquals(ch_dt_enum8, getGanttKey(chDtDateEnum8, ch_dt_enum8));

    List<GanttColumn> chDtEnum8Enum16 = getGanttColumn(tableName, chDtEnum8, chDtEnum16);
    assertEquals(ch_dt_enum8, chDtEnum8Enum16.get(0).getKey());
    assertEquals(ch_dt_enum16, getGanttKey(chDtEnum8Enum16, ch_dt_enum16));

    List<GanttColumn> chDtEnum16Int1 = getGanttColumn(tableName, chDtEnum16, chDtInt1);
    assertEquals(ch_dt_enum16, chDtEnum16Int1.get(0).getKey());
    assertEquals(ch_dt_int1, Byte.valueOf(getGanttKey(chDtEnum16Int1, String.valueOf(ch_dt_int1))));

    List<GanttColumn> chDtInt1Int8 = getGanttColumn(tableName, chDtInt1, chDtInt8);
    assertEquals(ch_dt_int1, Byte.valueOf(chDtInt1Int8.get(0).getKey()));
    assertEquals(ch_dt_int8, Byte.valueOf(getGanttKey(chDtInt1Int8, String.valueOf(ch_dt_int8))));

    List<GanttColumn> chDtInt8Real = getGanttColumn(tableName, chDtInt8, chDtReal);
    assertEquals(ch_dt_int8, Byte.valueOf(chDtInt8Real.get(0).getKey()));
    assertEquals(ch_dt_real, Float.parseFloat(getGanttKey(chDtInt8Real, getStackedColumnKey(tableName, chDtReal))));

    /* TODO Approve with CSV
    List<GanttColumn> chDtRealText = getGanttColumn(tableName, chDtReal, chDtText);
    assertEquals(ch_dt_real, Float.parseFloat(chDtRealText.get(0).getKey()));
    assertEquals(ch_dt_text, getGanttKey(chDtRealText, ch_dt_text));*/

    List<GanttColumn> chDtTimeUuid = getGanttColumn(tableName, chDtTime, chDtUuid);
    Date dateTimeGC = formatter.parse(chDtTimeUuid.get(0).getKey());
    LocalDateTime localDateTimeTimeGC = new java.sql.Timestamp(dateTimeGC.getTime()).toLocalDateTime();
    assertEquals(ch_dt_time.minusHours(3), localDateTimeTimeGC); // TODO Investigate it
    assertEquals(ch_dt_uuid, UUID.fromString(getGanttKey(chDtTimeUuid, ch_dt_uuid.toString())));

    List<GanttColumn> chDtUint8Enum8_2 = getGanttColumn(tableName, chDtUint8, chDtEnum8_2);
    assertEquals(ch_dt_uint8, Short.valueOf(chDtUint8Enum8_2.get(0).getKey()));
    assertEquals(ch_dt_enum8_2, getGanttKey(chDtUint8Enum8_2, ch_dt_enum8_2));

    List<GanttColumn> chDtInt16Int32 = getGanttColumn(tableName, chDtInt16, chDtInt32);
    assertEquals(ch_dt_int16, Short.valueOf(chDtInt16Int32.get(0).getKey()));
    assertEquals(ch_dt_int32, Long.valueOf(getGanttKey(chDtInt16Int32, String.valueOf(ch_dt_int32))));

    List<GanttColumn> chDtInt32Int64 = getGanttColumn(tableName, chDtInt32, chDtINT64);
    assertEquals(ch_dt_int32, Long.valueOf(chDtInt32Int64.get(0).getKey()));
    assertEquals(ch_dt_int64, Long.valueOf(getGanttKey(chDtInt32Int64, String.valueOf(ch_dt_int64))));

    List<GanttColumn> chDtDoubleDate32 = getGanttColumn(tableName, chDtDouble, chDtDate32);
    assertEquals(ch_dt_double, Double.parseDouble(chDtDoubleDate32.get(0).getKey()));
    LocalDate ch_dt_date_32GC = Instant.ofEpochMilli(Long.parseLong(getGanttKey(chDtDoubleDate32, getStackedColumnKey(tableName, chDtDate32))) * 1000).atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(ch_dt_date32, ch_dt_date_32GC);

    List<GanttColumn> chDtEnum16_2Int128 = getGanttColumn(tableName, chDtEnum16_2, chDtInt128);
    assertEquals(ch_dt_enum16_2, chDtEnum16_2Int128.get(0).getKey());
    assertEquals(ch_dt_int128, getGanttKey(chDtEnum16_2Int128, ch_dt_int128));

    List<GanttColumn> chDtInt256Uint16 = getGanttColumn(tableName, chDtInt256, chDtUint16);
    assertEquals(ch_dt_int256, chDtInt256Uint16.get(0).getKey());
    assertEquals(ch_dt_uint16, Integer.valueOf(getGanttKey(chDtInt256Uint16, String.valueOf(ch_dt_uint16))));

    List<GanttColumn> chDtUint32Decimal = getGanttColumn(tableName, chDtUint32, chDtDecimal);
    assertEquals(ch_dt_uint32, Long.valueOf(chDtUint32Decimal.get(0).getKey()));
    assertEquals(ch_dt_decimal, new BigDecimal(getGanttKey(chDtUint32Decimal, ch_dt_decimal.toString())).setScale(2, RoundingMode.HALF_UP));

    List<GanttColumn> chDtFloat32Float64 = getGanttColumn(tableName, chDtFloat32, chDtFloat64);
    assertEquals(ch_dt_float32, Float.parseFloat(chDtFloat32Float64.get(0).getKey()));
    assertEquals(String.format("%.2f", ch_dt_float64), String.format("%.2f", Double.parseDouble(getGanttKey(chDtFloat32Float64, getStackedColumnKey(tableName, chDtFloat64)))));

    List<GanttColumn> chDtIntegerNumeric = getGanttColumn(tableName, chDtInteger, chDtNumeric);
    assertEquals(ch_dt_integer, Integer.valueOf(chDtIntegerNumeric.get(0).getKey()));
    assertEquals(ch_dt_numeric, new BigDecimal(getGanttKey(chDtIntegerNumeric, ch_dt_numeric.toString())).setScale(2, RoundingMode.HALF_UP));

    /* TODO Approve with CSV
    List<GanttColumn> chDtVarcharBoolean = getGanttColumn(tableName, chDtVarchar, chDtBoolean);
    assertEquals(ch_dt_varchar, chDtVarcharBoolean.get(0).getKey());
    assertEquals(ch_dt_boolean, Boolean.valueOf(getGanttKey(chDtVarcharBoolean, String.valueOf(ch_dt_boolean))));*/

    List<GanttColumn> chDtDatetimeNvarchar = getGanttColumn(tableName, chDtDatetime, chDtNvarchar);
    Date dateTimeVarcharGC = formatter.parse(chDtDatetimeNvarchar.get(0).getKey());
    LocalDateTime localDateTimeTimeVarcharGC = new java.sql.Timestamp(dateTimeVarcharGC.getTime()).toLocalDateTime();
    assertEquals(ch_dt_datetime, localDateTimeTimeVarcharGC);
    assertEquals(ch_dt_nvarchar, getGanttKey(chDtDatetimeNvarchar, ch_dt_nvarchar));

    List<GanttColumn> chDtSmallintTimestamp = getGanttColumn(tableName, chDtSmallint, chDtFixedstring);
    assertEquals(ch_dt_smallint, Short.valueOf(chDtSmallintTimestamp.get(0).getKey()));
    assertEquals(ch_dt_fixedstring, getGanttKey(chDtSmallintTimestamp, ch_dt_fixedstring).trim());

    /* Test Raw data API */
    List<List<Object>> rawDataAll = fStore.getRawDataAll(tableName, 0, Long.MAX_VALUE);

    rawDataAll.forEach(row -> cProfiles.forEach(cProfile -> {
      try {
        if (cProfile.equals(chDtBit)) {
          assertEquals(ch_dt_bit, Integer.valueOf(getStackedColumnKey(tableName, chDtBit)));
        } else if (cProfile.equals(chDtDec)) {
          assertEquals(ch_dt_dec, new BigDecimal(getStackedColumnKey(tableName, chDtDec)).setScale(2, RoundingMode.HALF_UP));
        } else if (cProfile.equals(chDtInt)) {
          assertEquals(ch_dt_int, Integer.valueOf(getStackedColumnKey(tableName, chDtInt)));
        } else if (cProfile.equals(chDtByte)) {
          assertEquals(ch_dt_byte, Byte.valueOf(getStackedColumnKey(tableName, chDtByte)));
        } else if (cProfile.equals(chDtBool)) {
          assertEquals(ch_dt_bool, Boolean.valueOf(getStackedColumnKey(tableName, chDtBool)));
        } else if (cProfile.equals(chDtChar)) {
          assertEquals(ch_dt_char, getStackedColumnKey(tableName, chDtChar));
        } else if (cProfile.equals(chDtDate)) {
          LocalDate ch_dt_date_ld_raw = Instant.ofEpochMilli(Long.parseLong(getStackedColumnKey(tableName, chDtDate)) * 1000)
              .atZone(ZoneId.systemDefault()).toLocalDate();
          assertEquals(ch_dt_date, ch_dt_date_ld_raw);
        } else if (cProfile.equals(chDtEnum8)) {
          assertEquals(ch_dt_enum8, getStackedColumnKey(tableName, chDtEnum8));
        }  if (cProfile.equals(chDtEnum16)) {
          assertEquals(ch_dt_enum16, getStackedColumnKey(tableName, chDtEnum16));
        } else if (cProfile.equals(chDtInt1)) {
          assertEquals(ch_dt_int1, Byte.valueOf(getStackedColumnKey(tableName, chDtInt1)));
        } else if (cProfile.equals(chDtInt8)) {
          assertEquals(ch_dt_int8, Byte.valueOf(getStackedColumnKey(tableName, chDtInt8)));
        } else if (cProfile.equals(chDtReal)) {
          assertEquals(ch_dt_real, Float.parseFloat(getStackedColumnKey(tableName, chDtReal)));
        } else if (cProfile.equals(chDtTime)) {
          Date dateTimeRaw = formatter.parse(getStackedColumnKey(tableName, chDtTime));
          LocalDateTime localDateTimeTimeRaw = new java.sql.Timestamp(dateTimeRaw.getTime()).toLocalDateTime();
          assertEquals(ch_dt_time.minusHours(3), localDateTimeTimeRaw);
        } else if (cProfile.equals(chDtUuid)) {
          assertEquals(ch_dt_uuid.toString(), getStackedColumnKey(tableName, chDtUuid));
        } else if (cProfile.equals(chDtUint8)) {
          assertEquals(ch_dt_uint8, Short.valueOf(getStackedColumnKey(tableName, chDtUint8)));
        } else if (cProfile.equals(chDtEnum8_2)) {
          assertEquals(ch_dt_enum8_2, getStackedColumnKey(tableName, chDtEnum8_2));
        } else if (cProfile.equals(chDtInt16)) {
          assertEquals(ch_dt_int16, Short.valueOf(getStackedColumnKey(tableName, chDtInt16)));
        } else if (cProfile.equals(chDtInt32)) {
          assertEquals(ch_dt_int32, Long.valueOf(getStackedColumnKey(tableName, chDtInt32)));
        } else if (cProfile.equals(chDtINT64)) {
          assertEquals(ch_dt_int64, Long.valueOf(getStackedColumnKey(tableName, chDtINT64)));
        } else if (cProfile.equals(chDtDouble)) {
          assertEquals(ch_dt_double, Double.valueOf(getStackedColumnKey(tableName, chDtDouble)));
        } else if (cProfile.equals(chDtDate32)) {
          LocalDate ch_dt_date_32_raw = Instant.ofEpochMilli(Long.parseLong(getStackedColumnKey(tableName, chDtDate32)) * 1000).atZone(ZoneId.systemDefault()).toLocalDate();
          assertEquals(ch_dt_date32, ch_dt_date_32_raw);
        }

      } catch (Exception e) {
        log.info(e.getMessage());
        throw new RuntimeException(e);
      }
    }));
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

  private CProfile getCProfile(List<CProfile> cProfiles, String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow();
  }
}
