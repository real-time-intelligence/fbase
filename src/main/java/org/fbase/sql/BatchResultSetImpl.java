package org.fbase.sql;

import static org.fbase.service.CommonServiceApi.transpose;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.storage.RawDAO;

public class BatchResultSetImpl implements BatchResultSet {
  private final String tableName;
  private final byte tableId;
  private final int fetchSize;
  private final List<CProfile> cProfiles;

  private final RawDAO rawDAO;

  private Map.Entry<Long, Integer> pointer = Map.entry(0L, 0);

  private boolean isNext = true;
  private boolean isStarted = true;

  private long maxKey;

  /**
   * Constructor
   *
   * @param tableName table name
   * @param tableId table id
   * @param fetchSize the number of rows to fetch
   * @param cProfiles list of column profiles
   * @param rawDAO DAO for raw data
   */
  public BatchResultSetImpl(String tableName, byte tableId, int fetchSize, List<CProfile> cProfiles, RawDAO rawDAO) {
    this.tableName = tableName;
    this.tableId = tableId;
    this.fetchSize = fetchSize;
    this.cProfiles = cProfiles;
    this.rawDAO = rawDAO;

    this.maxKey = rawDAO.getMaxKey(tableId);
  }

  @Override
  public List<List<Object>> getObject() {
    List<List<Object>> columnDataListLocal = new ArrayList<>();

    AtomicReference<Entry<Long, Integer>> pointerLocal = new AtomicReference<>(Map.entry(0L, 0));

    cProfiles.stream()
        .sorted(Comparator.comparing(CProfile::getColId))
        .toList()
        .stream()
        .filter(f -> f.getCsType().getSType() == SType.RAW)
        .forEach(cProfile -> {
          AtomicInteger fetchCounter = new AtomicInteger(fetchSize);
          Map.Entry<Map.Entry<Long, Integer>, List<Object>> columnData =
              rawDAO.getColumnData(tableId, cProfile.getColId(), cProfile.getCsType().getCType(),
                  fetchSize, isStarted, pointer, fetchCounter);

          pointerLocal.set(columnData.getKey());

          columnDataListLocal.add(cProfile.getColId(), columnData.getValue());
        });

    pointer = pointerLocal.get();

    isStarted = false;

    if (pointer.getKey() > maxKey) isNext = false;

    return transpose(columnDataListLocal);
  }

  @Override
  public boolean next() {
    return isNext;
  }

}
