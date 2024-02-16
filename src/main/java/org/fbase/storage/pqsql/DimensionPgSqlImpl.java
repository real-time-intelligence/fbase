package org.fbase.storage.pqsql;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.storage.DimensionDAO;

@Log4j2
public class DimensionPgSqlImpl implements DimensionDAO {

  private final BasicDataSource basicDataSource;

  public DimensionPgSqlImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public int getOrLoad(double value) {
    return 0;
  }

  @Override
  public int getOrLoad(String value) {
    return 0;
  }

  @Override
  public String getStringById(int key) {
    return "";
  }

  @Override
  public double getDoubleById(int key) {
    return 0D;
  }
}
