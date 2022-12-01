package org.fbase.model.profile.cstype;

public enum SType {
  RAW(1, "Raw data"),
  HISTOGRAM(2, "Histograms value"),
  ENUM(3, "Predefined list of constants");

  private final byte key;
  private final String value;

  SType(int key, String value) {
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
