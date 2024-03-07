package org.fbase.sql;

import static org.fbase.service.mapping.Mapper.convertRawToLong;
import static org.fbase.service.mapping.Mapper.convertRawToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.service.CommonServiceApi;
import org.fbase.storage.dialect.DatabaseDialect;

@Log4j2
public class BatchResultSetSqlImpl extends CommonServiceApi implements BatchResultSet {

  private final String tableName;

  private final long begin;
  private final long end;

  private final int fetchSize;
  private final List<CProfile> cProfiles;

  private Entry<Long, Integer> pointer;

  private boolean isNext = true;
  private boolean isStarted = true;

  private final long maxBlockId;

  private final BasicDataSource basicDataSource;

  private final DatabaseDialect databaseDialect;

  /**
   * A ResultSet object maintains a cursor pointing to rows of data by fetchSize (JDBC)
   *
   * @param tableName       table name
   * @param fetchSize       the number of rows to fetch
   * @param begin           the start range
   * @param end             the end range
   * @param maxBlockId      the max value up to end
   * @param cProfiles       list of column profiles
   * @param basicDataSource data source with JDBC connection's
   */
  public BatchResultSetSqlImpl(String tableName,
                               int fetchSize,
                               long begin,
                               long end,
                               long maxBlockId,
                               List<CProfile> cProfiles,
                               BasicDataSource basicDataSource,
                               DatabaseDialect databaseDialect) {
    this.tableName = tableName;
    this.fetchSize = fetchSize;
    this.begin = begin;
    this.end = end;
    this.cProfiles = cProfiles;
    this.basicDataSource = basicDataSource;
    this.databaseDialect = databaseDialect;

    this.pointer = Map.entry(begin, 0);

    this.maxBlockId = maxBlockId;
  }

  @Override
  public List<List<Object>> getObject() {
    List<List<Object>> tableColFormatData = new ArrayList<>();

    AtomicReference<Entry<Long, Integer>> pointerLocal
        = new AtomicReference<>(Map.entry(isStarted ? 0L : pointer.getKey(), isStarted ? 0 : pointer.getValue()));

    Optional<CProfile> tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny();

    if (tsCProfile.isEmpty()) {
      throw new RuntimeException("API working only for time-series tables");
    }

    String query = "SELECT * FROM " + tableName + " "
        + databaseDialect.getWhereClass(tsCProfile.get(), null, null)
        + databaseDialect.getOrderByClass(tsCProfile.get());

    AtomicInteger fetchCounter = new AtomicInteger(pointer.getValue());

    try (Connection connection = basicDataSource.getConnection();
        PreparedStatement ps = connection.prepareStatement(query)) {

      long dateTime = isStarted ? pointer.getKey() : pointer.getKey() + 1;
      databaseDialect.setDateTime(tsCProfile.get(), ps, 1, dateTime);
      databaseDialect.setDateTime(tsCProfile.get(), ps, 2, maxBlockId);

      ResultSet rs = ps.executeQuery();

      for (int i = 0; i < cProfiles.size(); i++) {
        tableColFormatData.add(new ArrayList<>());
      }

      while (rs.next()) {
        for (int i = 0; i < cProfiles.size(); i++) {
          CProfile cProfile = cProfiles.get(i);
          Object cellValue = rs.getObject(cProfile.getColName());

          if (cProfile.getCsType().getCType().equals(CType.STRING)) {
            tableColFormatData.get(i).add(convertRawToString(cellValue, cProfile));
          } else {
            tableColFormatData.get(i).add(cellValue);
          }

          if (cProfiles.get(i).getColName().equals(tsCProfile.get().getColName())) {
            long value = convertRawToLong(cellValue, tsCProfile.get());
            pointerLocal.set(Map.entry(value, fetchCounter.incrementAndGet()));
          }
        }

        if (fetchCounter.get() >= fetchSize) {
          break;
        }
      }
    } catch (SQLException e) {
      log.catching(e);
    }

    if (fetchCounter.get() >= fetchSize) {
      fetchCounter.set(0);
      pointer = Map.entry(pointerLocal.get().getKey(), 0);
    } else {
      pointer = pointerLocal.get();
    }

    isStarted = false;

    if (pointer.getKey() >= maxBlockId) {
      isNext = false;
    }

    return transpose(tableColFormatData);
  }

  @Override
  public boolean next() {
    return isNext;
  }
}
