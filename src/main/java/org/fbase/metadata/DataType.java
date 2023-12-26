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

  /* ClickHouse */
  DEC(47, "DEC"),
  BYTE(48, "BYTE"),
  ENUM(49, "ENUM"),
  INT1(51, "INT1"),
  ENUM8(52, "ENUM8"),
  INT16(53, "INT16"),
  INT32(54, "INT32"),
  INT64(55, "INT64"),
  UINT8(56, "UINT8"),
  DATE32(57, "DATE32"),
  ENUM16(58, "ENUM16"),
  INT128(59, "INT128"),
  INT256(60, "INT256"),
  UINT16(61, "UINT16"),
  UINT32(62, "UINT32"),
  UINT64(63, "UINT64"),
  FLOAT32(64, "FLOAT32"),
  FLOAT64(65, "FLOAT64"),
  BOOLEAN(66, "BOOLEAN"),
  FIXEDSTRING(67, "FIXEDSTRING"),
  NULLABLE(68, "NULLABLE"),
  IPV4(69, "IPV4"),
  IPV6(70, "IPV6"),
  ARRAY(71, "ARRAY"),
  MAP(72, "MAP"),

  INTEGER(122, "INTEGER"),
  OID(123, "OID"),
  NAME(124, "NAME"),

  // CSV
  LONG(125, "LONG"),
  DOUBLE(126, "DOUBLE"),
  STRING(127, "STRING");

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
