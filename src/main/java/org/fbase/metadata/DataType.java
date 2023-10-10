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

  INTEGER(32, "INTEGER"),
  OID(33, "OID"),
  NAME(34, "NAME"),

  // ClickHouse
  UINT32(35, "UINT32"),
  ENUM8(36, "ENUM8"),
  UINT8(37, "UINT8"),
  DATETIME(38, "DATETIME"),
  FLOAT64(39, "FLOAT64"),
  FLOAT32(40, "FLOAT32"),
  FIXEDSTRING(41, "FIXEDSTRING"),
  ENUM16(42, "ENUM16"),
  UINT16(43, "UINT16"),

  // CSV
  LONG(44, "LONG"),
  DOUBLE(45, "DOUBLE"),
  STRING(46, "STRING"),

  // PG

  // MS SQL
  SMALLINT(47, "SMALLINT"),
  INT(48, "INT"),
  BIGINT(49, "BIGINT"),
  NVARCHAR(50, "NVARCHAR");

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
