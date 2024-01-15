package org.fbase.service;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.sql.BatchResultSet;

public interface RawService {

  List<StackedColumn> getListStackedColumn(String tableName,
                                           CProfile cProfile,
                                           long begin,
                                           long end)
      throws SqlColMetadataException;

  List<List<Object>> getRawDataAll(String tableName,
                                   long begin,
                                   long end);

  List<List<Object>> getRawDataByColumn(String tableName,
                                        CProfile cProfile,
                                        long begin,
                                        long end);

  BatchResultSet getBatchResultSet(String tableName,
                                   long begin,
                                   long end,
                                   int fetchSize);

  Entry<Entry<Long, Integer>, List<Object>> getColumnData(byte tableId,
                                                          int colIndex,
                                                          int tsColIndex,
                                                          CProfile cProfile,
                                                          int fetchSize,
                                                          boolean isStarted,
                                                          long maxBlockId,
                                                          Entry<Long, Integer> pointer,
                                                          AtomicInteger fetchCounter);

  long getMaxBlockId(byte tableId);

  long getLastTimestamp(String tableName,
                        long begin,
                        long end);
}
