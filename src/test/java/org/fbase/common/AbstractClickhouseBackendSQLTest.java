package org.fbase.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.FBase;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractClickhouseBackendSQLTest implements JdbcSource {

  @TempDir
  static File databaseDir;

  protected final String DB_URL = "jdbc:clickhouse://localhost:8123";

  protected BasicDataSource basicDataSource;

  protected FBaseConfig fBaseConfig;
  protected FBase fBase;
  protected FStore fStore;

  protected final String tableName = "datasets.trips_mergetree";

  private ObjectMapper objectMapper;

  @BeforeAll
  public void initBackendAndLoad() {
    try {
      System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");

      basicDataSource = getDatasource();

      fBaseConfig = new FBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath()).setBlockSize(16);
      fBase = new FBase(fBaseConfig, BType.CLICKHOUSE, getDatasource());
      fStore = fBase.getFStore();

      objectMapper = new ObjectMapper();
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  protected SProfile getSProfileForBackend(String select) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForSelect(select, basicDataSource.getConnection()).getCsTypeMap().forEach((key, value) -> {
      if (key.equals("PICKUP_DATE")) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(false).sType(SType.RAW).build());
      }
    });

    return new SProfile().setTableName(tableName)
        .setTableType(TType.TIME_SERIES)
        .setIndexType(IType.GLOBAL)
        .setBackendType(BType.CLICKHOUSE)
        .setCompression(false)
        .setCsTypeMap(csTypeMap);
  }

  private BasicDataSource getDatasource() {
    BasicDataSource basicDataSource = null;
    try {
      basicDataSource = new BasicDataSource();

      basicDataSource.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
      basicDataSource.setUrl(DB_URL);

      basicDataSource.setValidationQueryTimeout(5);
      basicDataSource.setValidationQuery("SELECT 1");
      basicDataSource.addConnectionProperty("compress", "0");

      basicDataSource.setInitialSize(3);
      basicDataSource.setMaxTotal(7);
      basicDataSource.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(5));

    } catch (Exception e) {
      log.catching(e);
    }

    return basicDataSource;
  }

  public void assertStackedListEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    assertEquals(expected.stream().findAny().orElseThrow().getKeyCount(),
                 actual.stream().findAny().orElseThrow().getKeyCount());
  }

  public void assertGanttListEquals(List<GanttColumn> expected, List<GanttColumn> actual) {
    assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStackedTestData(fileName), new TypeReference<>() {});
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "stacked", fileName));
  }

  @AfterAll
  public void closeDb() throws IOException {
  }
}
