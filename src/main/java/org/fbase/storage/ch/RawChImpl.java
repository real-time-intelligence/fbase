package org.fbase.storage.ch;

import static org.fbase.service.mapping.Mapper.convertRawToLong;
import static org.fbase.storage.helper.ClickHouseHelper.enumParser;
import static org.fbase.storage.helper.ClickHouseHelper.getDateTime;

import com.sleepycat.persist.EntityCursor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.util.CachedLastLinkedHashMap;

@Log4j2
public class RawChImpl extends QueryBdbApi implements RawDAO {

  private final MetaModelApi metaModelApi;

  private final BasicDataSource basicDataSource;

  private final Map<Byte, Metadata> primaryIndex;

  public RawChImpl(MetaModelApi metaModelApi,
                   BasicDataSource basicDataSource) {
    this.metaModelApi = metaModelApi;
    this.basicDataSource = basicDataSource;

    this.primaryIndex = new HashMap<>();
  }

  @Override
  public void putMetadata(byte tableId,
                          long blockId,
                          byte[] rawCTypeKeys,
                          int[] rawColIds,
                          int[] enumColIds,
                          int[] histogramColIds) {
    MetadataKey metadataKey = new MetadataKey(tableId, blockId);
    this.primaryIndex.put(tableId, new Metadata(metadataKey, rawCTypeKeys, rawColIds, enumColIds, histogramColIds));
  }

  @Override
  public void putByte(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putInt(byte tableId,
                     long blockId,
                     int[] mapping,
                     int[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putLong(byte tableId,
                      long blockId,
                      int[] mapping,
                      long[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putFloat(byte tableId,
                       long blockId,
                       int[] mapping,
                       float[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putDouble(byte tableId,
                        long blockId,
                        int[] mapping,
                        double[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putString(byte tableId,
                        long blockId,
                        int[] mapping,
                        String[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putEnum(byte tableId,
                      long blockId,
                      int[] mapping,
                      byte[][] data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            List<Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            List<Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            List<Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            List<Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            List<Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            List<Integer> rawDataStringMapping,
                            List<List<String>> rawDataString) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void putCompressed(byte tableId,
                            long blockId,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataTimeStampMapping,
                            List<List<Long>> rawDataTimestamp,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataIntMapping,
                            List<List<Integer>> rawDataInt,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataLongMapping,
                            List<List<Long>> rawDataLong,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataFloatMapping,
                            List<List<Float>> rawDataFloat,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataDoubleMapping,
                            List<List<Double>> rawDataDouble,
                            CachedLastLinkedHashMap<Integer, Integer> rawDataStringMapping,
                            List<List<String>> rawDataString) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public byte[] getRawByte(byte tableId,
                           long blockId,
                           int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public int[] getRawInt(byte tableId,
                         long blockId,
                         int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public long[] getRawLong(byte tableId,
                           long blockId,
                           int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public float[] getRawFloat(byte tableId,
                             long blockId,
                             int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public double[] getRawDouble(byte tableId,
                               long blockId,
                               int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public String[] getRawString(byte tableId,
                               long blockId,
                               int colId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Long> getListBlockIds(byte tableId,
                                    long begin,
                                    long end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public EntityCursor<Metadata> getMetadataEntityCursor(MetadataKey begin,
                                                        MetadataKey end) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Metadata getMetadata(MetadataKey metadataKey) {
    Metadata metadata = primaryIndex.get(metadataKey.getTableId());

    if (metadata == null) {
      log.info("No data found for metadata key -> " + metadataKey);
      return new Metadata();
    }

    return metadata;
  }

  @Override
  public long getPreviousBlockId(byte tableId,
                                 long blockId) {
    return getLastBlockIdLocal(tableId, 0L, blockId);
  }

  @Override
  public long getLastBlockId(byte tableId) {
    return getLastBlockIdLocal(tableId, 0L, Long.MAX_VALUE);
  }

  @Override
  public long getLastBlockId(byte tableId,
                             long begin,
                             long end) {
    return getLastBlockIdLocal(tableId, begin, end);
  }

  @Override
  public List<StackedColumn> getListStackedColumn(String tableName,
                                                  CProfile tsCProfile,
                                                  CProfile cProfile,
                                                  CProfile cProfileFilter,
                                                  String filter,
                                                  long begin,
                                                  long end) {
    List<StackedColumn> results = new ArrayList<>();

    String beginDateTime = getDateTime(begin);
    String endDateTime = getDateTime(end);

    String colName = cProfile.getColName().toLowerCase();
    String tsColName = tsCProfile.getColName().toLowerCase();

    String query =
        "SELECT " + colName + ", COUNT(" + colName + ") " +
        "FROM " + tableName + " " +
        "WHERE " + tsColName + " BETWEEN toDate(?) AND toDate(?) " + getFilterAndString(cProfileFilter, filter) +
        "GROUP BY " + colName;

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {
      ps.setString(1, beginDateTime);
      ps.setString(2, endDateTime);

      ResultSet rs = ps.executeQuery();

      StackedColumn column = new StackedColumn();
      column.setKey(begin);
      column.setTail(end);

      while (rs.next()) {
        String key = rs.getString(1);
        int count = rs.getInt(2);

        column.getKeyCount().put(key, count);
      }

      results.add(column);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return results;
  }

  private String getFilterAndString(CProfile cProfileFilter,
                                    String filter) {
    if (cProfileFilter != null) {
      AtomicReference<String> filterAndString = new AtomicReference<>("");

      if (cProfileFilter.getColDbTypeName().startsWith("ENUM")) {
        Map<String, Integer> stringIntegerMap = enumParser(cProfileFilter.getColDbTypeName());
        stringIntegerMap.forEach((k, v) -> {
          if (k.equals(filter)) {
            filterAndString.set(" AND " + cProfileFilter.getColName().toLowerCase() + " = '" + v + "' ");
          }
        });
      } else {
        filterAndString.set(" AND " + cProfileFilter.getColName().toLowerCase() + " = '" + filter + "' ");
      }

      return filterAndString.get();
    } else {
      return "";
    }
  }

  private long getLastBlockIdLocal(byte tableId,
                                   long begin,
                                   long end) {
    long lastBlockId = 0L;

    String beginDateTime = getDateTime(begin);
    String endDateTime = getDateTime(end);

    String tableName = metaModelApi.getTableName(tableId);
    List<CProfile> cProfiles = metaModelApi.getCProfiles(tableId);

    CProfile tsCProfile = cProfiles.stream()
        .filter(k -> k.getCsType().isTimeStamp())
        .findAny()
        .orElseThrow(() -> new RuntimeException("Not found timestamp column"));

    String query = "";

    String colName = tsCProfile.getColName().toLowerCase();
    if (Long.MAX_VALUE == end) {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName;
    } else {
      query =
          "SELECT MAX(" + colName + ") " +
              "FROM " + tableName + " " +
              "WHERE " + colName + " BETWEEN toDate(?) AND toDate(?) ";
    }

    try (Connection conn = basicDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

      if (Long.MAX_VALUE == end) {

      } else {
        ps.setString(1, beginDateTime);
        ps.setString(2, endDateTime);
      }

      ResultSet rs = ps.executeQuery();

      StackedColumn column = new StackedColumn();
      column.setKey(begin);
      column.setTail(end);

      while (rs.next()) {
        Object object = rs.getObject(1);

        lastBlockId = convertRawToLong(object, tsCProfile);
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return lastBlockId;
  }
}