package org.fbase.storage.ch;

import com.sleepycat.persist.EntityCursor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.dialect.DatabaseDialect;
import org.fbase.storage.common.QueryJdbcApi;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.entity.Metadata;
import org.fbase.storage.bdb.entity.MetadataKey;
import org.fbase.storage.dialect.ClickHouseDialect;
import org.fbase.util.CachedLastLinkedHashMap;

@Log4j2
public class RawChImpl extends QueryJdbcApi implements RawDAO {

  private final MetaModelApi metaModelApi;
  private final DatabaseDialect databaseDialect;

  private final Map<Byte, Metadata> primaryIndex;

  public RawChImpl(MetaModelApi metaModelApi,
                   BasicDataSource basicDataSource) {
    super(basicDataSource);

    this.metaModelApi = metaModelApi;

    this.databaseDialect = new ClickHouseDialect();
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
    return getListStackedColumn(tableName, tsCProfile, cProfile, cProfileFilter, filter, begin, end, databaseDialect);
  }

  @Override
  public List<GanttColumn> getListGanttColumn(String tableName,
                                              CProfile tsCProfile,
                                              CProfile firstGrpBy,
                                              CProfile secondGrpBy,
                                              long begin,
                                              long end) {
    return getListGanttColumn(tableName, tsCProfile, firstGrpBy, secondGrpBy, begin, end, databaseDialect);
  }

  private long getLastBlockIdLocal(byte tableId,
                                   long begin,
                                   long end) {
    String tableName = metaModelApi.getTableName(tableId);
    CProfile tsCProfile = metaModelApi.getTimestampCProfile(tableName);

    return getLastBlockIdLocal(tableName, tsCProfile, begin, end, databaseDialect);
  }
}