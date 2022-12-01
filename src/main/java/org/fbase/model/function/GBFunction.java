package org.fbase.model.function;

public enum GBFunction {
  COUNT("COUNT"),
  SUM("SUM");

  private final String name;

  GBFunction(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

}
