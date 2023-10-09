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


  CLOB(1, "CLOB"),
  VARCHAR2(2, "VARCHAR2"),
  INTEGER(5, "INTEGER"),
  NUMBER(6, "NUMBER"),
  OID(10, "OID"),
  NAME(12, "NAME"),

  // ClickHouse
  UINT32(15, "UINT32"),
  ENUM8(16, "ENUM8"),
  UINT8(17, "UINT8"),
  DATETIME(18, "DATETIME"),
  FLOAT64(19, "FLOAT64"),
  FLOAT32(20, "FLOAT32"),
  FIXEDSTRING(20, "FIXEDSTRING"),
  ENUM16(21, "ENUM16"),
  UINT16(22, "UINT16"),
  RAW(23, "RAW"),

  // CSV
  LONG(24, "LONG"),
  DOUBLE(25, "DOUBLE"),
  STRING(26, "STRING"),

  // PG

  // MS SQL
  FLOAT(28, "FLOAT"),
  SMALLINT(29, "SMALLINT"),
  INT(30, "INT"),
  BIGINT(31, "BIGINT"),
  NVARCHAR(33, "NVARCHAR");

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
