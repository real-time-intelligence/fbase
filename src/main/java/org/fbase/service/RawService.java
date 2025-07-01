package org.fbase.service;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.BlockKeyTail;
import org.fbase.model.profile.CProfile;
import org.fbase.sql.BatchResultSet;

public interface RawService {

  List<List<Object>> getRawDataAll(String tableName,
                                   long begin,
                                   long end);

  List<List<Object>> getRawDataAll(String tableName,
                                   CProfile cProfileFilter,
                                   String filter,
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

  long getFirst(String tableName,
                long begin,
                long end);

  long getLast(String tableName,
               long begin,
               long end);

  List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                         long begin,
                                         long end) throws BeginEndWrongOrderException, SqlColMetadataException;
}
