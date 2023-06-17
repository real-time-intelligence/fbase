package org.fbase.metadata;

public enum DataType {
  CLOB(1, "CLOB"),
  VARCHAR2(2, "VARCHAR2"),
  CHAR(3, "CHAR"),
  INT4(4, "INT4"),
  INTEGER(5, "INTEGER"),
  NUMBER(6, "NUMBER"),
  DATE(7, "DATE"),
  TIMESTAMP(8, "TIMESTAMP"),
  FLOAT8(9, "FLOAT8"),
  OID(10, "OID"),
  TEXT(11, "TEXT"),
  NAME(12, "NAME"),
  TIMESTAMPTZ(13, "TIMESTAMPTZ"),
  VARCHAR(14, "VARCHAR"),

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
  NUMERIC(27, "NUMERIC"),

  // MS SQL
  FLOAT(28, "FLOAT"),
  SMALLINT(29, "SMALLINT"),
  INT(30, "INT"),
  BIGINT(31, "BIGINT"),
  BIT(32, "BIT"),
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
