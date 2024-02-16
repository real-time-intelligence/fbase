package org.fbase.storage.pqsql;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.bdb.QueryBdbApi;

@Log4j2
public class HistogramPgSqlImpl extends QueryBdbApi implements HistogramDAO {

  private final BasicDataSource basicDataSource;

  public HistogramPgSqlImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public void put(byte tableId,
                  long blockId,
                  int colId,
                  int[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            int colId,
                            int[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressedKeysValues(byte tableId,
                                      long blockId,
                                      int colId,
                                      int[] keys,
                                      int[] values) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public int[][] get(byte tableId,
                     long blockId,
                     int colId) {
    throw new RuntimeException("Not supported");
  }
}
