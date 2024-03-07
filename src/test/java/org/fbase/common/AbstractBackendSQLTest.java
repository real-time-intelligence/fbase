package org.fbase.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.util.Assert;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.FBase;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractBackendSQLTest implements JdbcSource {

  @TempDir
  static File databaseDir;

  protected FBaseConfig fBaseConfig;
  protected FBase fBase;
  protected FStore fStore;

  private ObjectMapper objectMapper;

  public void initMetaDataBackend(BType bType,
                                  BasicDataSource basicDataSource) {
    try {
      System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");

      fBaseConfig = new FBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath()).setBlockSize(16);
      fBase = new FBase(fBaseConfig, bType, basicDataSource);
      fStore = fBase.getFStore();

      objectMapper = new ObjectMapper();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  protected SProfile getSProfileForBackend(String tableName,
                                           BasicDataSource basicDataSource,
                                           BType bType,
                                           String select,
                                           String tsName) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, basicDataSource.getConnection()).getCsTypeMap().forEach((key, value) -> {
      if (key.equals(tsName)) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(false).sType(SType.RAW).build());
      }
    });

    return new SProfile().setTableName(tableName)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.GLOBAL)
        .setBackendType(bType)
        .setCompression(false)
        .setCsTypeMap(csTypeMap);
  }

  protected BasicDataSource getDatasource(BType bType,
                                          String driverClassName,
                                          String dbUrl,
                                          String username,
                                          String password) {
    BasicDataSource basicDataSource = null;
    try {
      basicDataSource = new BasicDataSource();

      basicDataSource.setDriverClassName(driverClassName);
      basicDataSource.setUrl(dbUrl);

      basicDataSource.setValidationQueryTimeout(5);

      if (username != null) {
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
      }

      if (BType.CLICKHOUSE.equals(bType)) {
        basicDataSource.setValidationQuery("SELECT 1");
        basicDataSource.addConnectionProperty("compress", "0");
      }

      basicDataSource.setInitialSize(3);
      basicDataSource.setMaxTotal(7);
      basicDataSource.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(5));

    } catch (Exception e) {
      log.catching(e);
    }

    return basicDataSource;
  }

  protected long getUnitTimestamp(LocalDateTime localDateTime) {
    ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
    return localDateTime.toInstant(offset).toEpochMilli();
  }

  protected static void dropTable(Connection connection,
                                  String tableName) throws SQLException {
    String sql = "DROP TABLE IF EXISTS " + tableName;

    try (Statement statement = connection.createStatement()) {
      System.out.println(sql);
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  protected static void dropTableOracle(Connection connection, String tableName) throws SQLException {
    String sql = "DROP TABLE " + tableName;

    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      log.info("Table dropped successfully!");
    }
  }

  protected static boolean tableExists(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    try (var resultSet = metaData.getTables(null, null, tableName, null)) {
      return resultSet.next();
    }
  }

  private String getGanttKey(List<GanttColumn> ganttColumnList,
                             String filter) {
    return ganttColumnList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> f.getKey().trim().equalsIgnoreCase(filter))
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private String getGanttKeyFloat(List<GanttColumn> ganttColumnList,
                                  String filter) {
    return ganttColumnList.get(0).getGantt()
        .entrySet()
        .stream()
        .filter(f -> {
          Float val = Float.valueOf(f.getKey());
          String valStr = String.format("%.2f", val);
          return valStr.equals(filter);
        })
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private List<GanttColumn> getGanttColumn(String tableName,
                                           CProfile cProfileFirst,
                                           CProfile cProfileSecond)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    return fStore.getGColumnListTwoLevelGroupBy(tableName, cProfileFirst, cProfileSecond, 0, Long.MAX_VALUE);
  }

  private String getStackedColumnKey(String tableName,
                                     CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tableName, cProfile, GroupFunction.COUNT, 0, Long.MAX_VALUE)
        .stream()
        .findAny()
        .orElseThrow()
        .getKeyCount()
        .entrySet()
        .stream()
        .findAny()
        .orElseThrow()
        .getKey();
  }

  private Optional<StackedColumn> getStackedColumn(String tableName,
                                                   CProfile cProfile)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tableName, cProfile, GroupFunction.COUNT, 0, Long.MAX_VALUE)
        .stream()
        .findAny();
  }

  private Optional<List<Object>> getRawDataByColumn(String tableName,
                                                    CProfile cProfile) {
    return fStore.getRawDataByColumn(tableName, cProfile, Long.MIN_VALUE, Long.MAX_VALUE)
        .stream()
        .findAny();
  }

  private CProfile getCProfile(List<CProfile> cProfiles,
                               String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow();
  }

  public void assertStackedListEquals(List<StackedColumn> expected,
                                      List<StackedColumn> actual) {
    assertEquals(expected.stream().findAny().orElseThrow().getKeyCount(),
                 actual.stream().findAny().orElseThrow().getKeyCount());
  }

  protected void assertGanttMapEquals(List<GanttColumn> expected,
                                      List<GanttColumn> actual) {
    expected.forEach(exp -> Assert.equals(exp.getGantt(), actual.stream()
        .filter(f -> f.getKey().equalsIgnoreCase(exp.getKey()))
        .findFirst()
        .orElseThrow()
        .getGantt()));
  }

  public void assertGanttListEquals(List<GanttColumn> expected,
                                    List<GanttColumn> actual) {
    assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
  }

  protected List<GanttColumn> getGanttDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getGanttTestData(fileName), new TypeReference<>() {
    });
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStackedTestData(fileName), new TypeReference<>() {
    });
  }

  private String getGanttTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "gantt", fileName));
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "stacked", fileName));
  }

  @AfterAll
  public void closeDb() throws IOException {
  }
}