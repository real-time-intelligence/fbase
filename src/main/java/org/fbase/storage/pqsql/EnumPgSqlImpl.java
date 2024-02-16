package org.fbase.storage.pqsql;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.column.EColumn;

@Log4j2
public class EnumPgSqlImpl extends QueryBdbApi implements EnumDAO {

  private final BasicDataSource basicDataSource;

  public EnumPgSqlImpl(BasicDataSource basicDataSource) {
    this.basicDataSource = basicDataSource;
  }

  @Override
  public EColumn putEColumn(byte tableId,
                            long blockId,
                            int colId,
                            int[] values,
                            byte[] data,
                            boolean compression) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public EColumn getEColumnValues(byte tableId,
                                  long blockId,
                                  int colId) {
    throw new RuntimeException("Not supported");
  }
}
