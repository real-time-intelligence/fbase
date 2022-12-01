package org.fbase.model.profile.cstype;

public enum CType {
  BYTE(1, "byte"),
  INT(2, "int"),
  LONG(3, "long"),
  FLOAT(4, "float"),
  DOUBLE(5, "double"),
  STRING(6, "string");

  private final byte key;
  private final String value;

  CType(int key, String value) {
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
