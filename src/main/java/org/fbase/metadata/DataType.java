package org.fbase.metadata;

public enum DataType {
  /* Postgres */
  BIT(0, "BIT"),
  BOOL(1, "BOOL"),
  CHAR(2, "CHAR"),
  DATE(3, "DATE"),
  INT2(4, "INT2"),
  INT4(5, "INT4"),
  INT8(6, "INT8"),
  TEXT(7, "TEXT"),
  TIME(8, "TIME"),
  UUID(9, "UUID"),
  BYTEA(10, "BYTEA"),
  MONEY(11, "MONEY"),
  BPCHAR(12, "BPCHAR"),
  FLOAT4(13, "FLOAT4"),
  FLOAT8(14, "FLOAT8"),
  SERIAL(15, "SERIAL"),
  TIMETZ(16, "TIMETZ"),
  NUMERIC(17, "NUMERIC"),
  VARCHAR(18, "VARCHAR"),
  BIGSERIAL(19, "BIGSERIAL"),
  TIMESTAMP(20, "TIMESTAMP"),
  SMALLSERIAL(21, "SMALLSERIAL"),
  TIMESTAMPTZ(22, "TIMESTAMPTZ"),

  /* Oracle */
  RAW(23, "RAW"),
  CLOB(24, "CLOB"),
  FLOAT(25, "FLOAT"),
  NCHAR(26, "NCHAR"),
  NCLOB(27, "NCLOB"),
  NUMBER(28, "NUMBER"),
  VARCHAR2(29, "VARCHAR2"),
  NVARCHAR2(30, "NVARCHAR2"),

  /* MS Sql */
  INT(31, "INT"),
  REAL(32, "REAL"),
  NTEXT(33, "NTEXT"),
  BIGINT(34, "BIGINT"),
  BINARY(35, "BINARY"),
  DECIMAL(36, "DECIMAL"),
  SYSNAME(37, "SYSNAME"),
  TINYINT(38, "TINYINT"),
  DATETIME(39, "DATETIME"),
  NVARCHAR(40, "NVARCHAR"),
  SMALLINT(41, "SMALLINT"),
  DATETIME2(42, "DATETIME2"),
  VARBINARY(43, "VARBINARY"),
  SMALLMONEY(44, "SMALLMONEY"),
  SMALLDATETIME(45, "SMALLDATETIME"),
  UNIQUEIDENTIFIER(46, "UNIQUEIDENTIFIER"),


  INTEGER(50, "INTEGER"),
  OID(51, "OID"),
  NAME(52, "NAME"),

  // ClickHouse
  UINT32(53, "UINT32"),
  ENUM8(54, "ENUM8"),
  UINT8(55, "UINT8"),
  //DATETIME(38, "DATETIME"),
  FLOAT64(56, "FLOAT64"),
  FLOAT32(57, "FLOAT32"),
  FIXEDSTRING(58, "FIXEDSTRING"),
  ENUM16(59, "ENUM16"),
  UINT16(60, "UINT16"),

  // CSV
  LONG(100, "LONG"),
  DOUBLE(101, "DOUBLE"),
  STRING(102, "STRING");

  private final byte key;
  private final String value;

  DataType(int key, String value) {
    this.key = (byte) key;
    this.value = value;
  }

  public byte getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }
}
