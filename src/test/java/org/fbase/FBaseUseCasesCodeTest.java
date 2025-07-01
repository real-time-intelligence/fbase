package org.fbase;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.mode.CSVMode;
import org.fbase.common.mode.DirectMode;
import org.fbase.common.mode.JDBCMode;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.metadata.DataType;
import org.fbase.model.CompareFunction;
import org.fbase.model.FBaseTestConfig;
import org.fbase.model.GroupFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.Person;
import org.fbase.model.output.BlockKeyTail;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.GanttColumnSum;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.source.H2Database;
import org.fbase.sql.BatchResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FBaseUseCasesCodeTest implements DirectMode, JDBCMode, CSVMode {

  @TempDir
  static File databaseDir;

  static File databaseDirDirect;
  static File databaseDirJdbc;
  static File databaseDirCsv;

  private FBaseTestConfig configDirect;
  private FBaseTestConfig configJdbc;
  private FBaseTestConfig configCsv;

  private static final String GET_TABLE_PROFILE_USE_CASE_TABLE_NAME = "table_name_get_table_profile_use_case_test";
  private static final String LOAD_DIRECT_METADATA_USE_CASE_TABLE_NAME = "table_name_load_direct_metadata_use_case_test";
  private static final String LOAD_JDBC_METADATA_USE_CASE_TABLE_NAME = "table_name_load_jdbc_metadata_use_case_test";
  private static final String LOAD_CSV_METADATA_USE_CASE_TABLE_NAME = "table_name_load_csv_metadata_use_case_test";
  private static final String SET_TIMESTAMP_COLUMN_USE_CASE_TABLE_NAME = "table_name_set_timestamp_column_use_case_test";
  private static final String PUT_DIRECT_USE_CASE_TABLE_NAME = "table_name_put_direct_use_case_test";
  private static final String PUT_JDBC_USE_CASE_TABLE_NAME = "table_name_put_jdbc_use_case_test";
  private static final String PUT_JDBC_BATCH_USE_CASE_TABLE_NAME = "table_name_put_jdbc_batch_use_case_test";
  private static final String GET_BLOCK_KEY_TAIL_USE_CASE_TABLE_NAME = "table_name_get_block_key_tail_list_use_case_test";
  private static final String GET_STACKED_USE_CASE_TABLE_NAME = "table_name_stacked_use_case_test";
  private static final String GET_STACKED_FILTER_USE_CASE_TABLE_NAME = "table_name_stacked_filter_use_case_test";
  private static final String GET_GANTT_USE_CASE_TABLE_NAME = "table_name_gantt_use_case_test";
  private static final String GET_GANTT_FILTER_USE_CASE_TABLE_NAME = "table_name_gantt_filter_use_case_test";
  private static final String GET_DISTINCT_USE_CASE_TABLE_NAME = "table_name_distinct_use_case_test";
  private static final String GET_RAW_USE_CASE_TABLE_NAME = "table_name_raw_use_case_test";
  private static final String GET_RAW_FILTER_USE_CASE_TABLE_NAME = "table_name_raw_filter_use_case_test";
  private static final String GET_RAW_COLUMN_USE_CASE_TABLE_NAME = "table_name_raw_column_use_case_test";
  private static final String GET_RESULT_SET_REGULAR_USE_CASE_TABLE_NAME = "table_name_result_set_regular_use_case_test";
  private static final String GET_RESULT_SET_TIME_SERIES_USE_CASE_TABLE_NAME = "table_name_result_time_series_set_use_case_test";
  private static final String GET_FIRST_TIMESTAMP_USE_CASE_TABLE_NAME = "table_name_first_timestamp_use_case_test";
  private static final String GET_LAST_TIMESTAMP_USE_CASE_TABLE_NAME = "table_name_last_timestamp_use_case_test";

  private H2Database h2Db;
  private String select = "SELECT * FROM person WHERE ROWNUM < 2";

  @BeforeAll
  public void initialization() throws SQLException, IOException {
    // TODO Fix bug - @TempDir created only one temp directory for class
    databaseDirDirect = createSubDirWithPostfix(databaseDir, "direct");
    databaseDirJdbc = createSubDirWithPostfix(databaseDir, "jdbc");
    databaseDirCsv = createSubDirWithPostfix(databaseDir, "csv");

    // Create test data using H2 database
    h2Db = new H2Database("jdbc:h2:mem:use_case_test");
    loadDataTablePersonH2(h2Db);

    // FBase initialization for Direct, JDBC and CSV mode
    configDirect = new FBaseTestConfig(databaseDirDirect, h2Db);
    configJdbc = new FBaseTestConfig(databaseDirJdbc, h2Db);
    configCsv = new FBaseTestConfig(databaseDirCsv, h2Db);
  }

  @Order(1)
  @Test
  public void getTProfile() throws TableNameEmptyException {
    log.info("Use case: get table metadata");

    loadMetadataDirect(configDirect, GET_TABLE_PROFILE_USE_CASE_TABLE_NAME, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, true);

    TProfile tProfile = configDirect.getFStore().getTProfile(GET_TABLE_PROFILE_USE_CASE_TABLE_NAME);

    assertEquals(GET_TABLE_PROFILE_USE_CASE_TABLE_NAME, tProfile.getTableName());

    log.info(tProfile);
  }

  @Order(2)
  @Test
  public void loadDirectTableMetadata() throws TableNameEmptyException {
    log.info("Use case: load table metadata in direct mode from SProfile configuration");

    SProfile sProfile = getSProfileDirect(LOAD_DIRECT_METADATA_USE_CASE_TABLE_NAME, TType.TIME_SERIES, IType.GLOBAL, BType.BERKLEYDB, true);

    TProfile tProfile = configDirect.getFStore().loadDirectTableMetadata(sProfile);

    assertEquals(LOAD_DIRECT_METADATA_USE_CASE_TABLE_NAME, tProfile.getTableName());

    log.info(tProfile);
  }

  @Order(3)
  @Test
  public void loadJdbcTableMetadata() throws TableNameEmptyException, SQLException {
    log.info("Use case: load table metadata from JDBC Connection");

    SProfile sProfile = getSProfileJdbc(LOAD_JDBC_METADATA_USE_CASE_TABLE_NAME, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, true);

    TProfile tProfile = configDirect.getFStore()
        .loadJdbcTableMetadata(configDirect.getH2DbConnection(), select, sProfile);

    assertEquals(LOAD_JDBC_METADATA_USE_CASE_TABLE_NAME, tProfile.getTableName());

    log.info(tProfile);
  }

  @Order(4)
  @Test
  public void loadCsvTableMetadata() throws TableNameEmptyException {
    log.info("Use case: load table metadata from CSV file");

    String fileName = new File("").getAbsolutePath() + FILE_SEPARATOR +
        Paths.get("src", "test", "resources", "csv", "file-l.csv");

    String csvSplitBy = ",";

    SProfile sProfile = getSProfileCsv(LOAD_CSV_METADATA_USE_CASE_TABLE_NAME, fileName, csvSplitBy, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, true);

    TProfile tProfile = configCsv.getFStore().loadCsvTableMetadata(fileName, csvSplitBy, sProfile);

    assertEquals(LOAD_CSV_METADATA_USE_CASE_TABLE_NAME, tProfile.getTableName());

    log.info(tProfile);
  }

  @Order(5)
  @Test
  public void setTimestampColumn() throws TableNameEmptyException {
    log.info("Use case: update table profile with new timestamp column");

    loadMetadataDirect(configDirect, SET_TIMESTAMP_COLUMN_USE_CASE_TABLE_NAME, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, true);

    TProfile tProfile = configDirect.getFStore().getTProfile(SET_TIMESTAMP_COLUMN_USE_CASE_TABLE_NAME);

    CProfile cProfileBeforeTimestamp = tProfile.getCProfiles()
        .stream()
        .filter(f -> f.getCsType().isTimeStamp())
        .findAny()
        .orElseThrow();
    CProfile cProfileAfterTimestamp = tProfile.getCProfiles()
        .stream()
        .filter(f -> f.getColName().equals("LONG_FIELD"))
        .findAny()
        .orElseThrow();

    assertTrue(cProfileBeforeTimestamp.getCsType().isTimeStamp());
    assertFalse(cProfileAfterTimestamp.getCsType().isTimeStamp());

    configDirect.getFStore().setTimestampColumn(SET_TIMESTAMP_COLUMN_USE_CASE_TABLE_NAME, cProfileAfterTimestamp.getColName());

    assertFalse(cProfileBeforeTimestamp.getCsType().isTimeStamp());
    assertTrue(cProfileAfterTimestamp.getCsType().isTimeStamp());

    log.info(tProfile);
  }

  @Order(6)
  @Test
  public void putDataDirect() throws TableNameEmptyException, EnumByteExceedException, SqlColMetadataException {
    log.info("Use case: save data in table using intermediate table structure");

    TProfile tProfile = setupDirectProfile(PUT_DIRECT_USE_CASE_TABLE_NAME, configDirect);

    List<List<Object>> data = prepareData();

    configDirect.getFStore().putDataDirect(PUT_DIRECT_USE_CASE_TABLE_NAME, data);

    assertData(tProfile.getTableName(), List.of(Arrays.asList(1707387748310L, 17073877482L, 17073877482D, "a31bce67-d1ab-485b-9ffa-850385e298ac")));

    log.info(tProfile);
  }

  @Order(7)
  @Test
  public void putDataJdbcBatch() throws TableNameEmptyException, SQLException {
    log.info("Use case: save data in table using JDBC connection");

    TProfile tProfile = setupJdbcProfile(PUT_JDBC_USE_CASE_TABLE_NAME);

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    assertRawDataJdbc(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    log.info(tProfile);
  }

  @Order(8)
  @Test
  public void putDataJdbc()
      throws TableNameEmptyException, EnumByteExceedException, SqlColMetadataException, SQLException {
    log.info("Use case: save data in table using JDBC connection");

    TProfile tProfile = setupJdbcProfile(PUT_JDBC_BATCH_USE_CASE_TABLE_NAME);

    putDataJdbcBatch(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    assertRawDataJdbc(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    log.info(tProfile);
  }

  @Order(9)
  @Test
  public void getBlockKeyTailList() throws Exception {
    log.info("Use case: get block key tail list");

    TProfile tProfile = setupJdbcProfile(GET_BLOCK_KEY_TAIL_USE_CASE_TABLE_NAME);

    putDataJdbcBatch(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    testBlockKeyTail(tProfile.getTableName(), 1, 3);
    testBlockKeyTail(tProfile.getTableName(), 1, 2);
    testBlockKeyTail(tProfile.getTableName(), 2, 3);

    List<StackedColumn> expectedBlock = List.of(
        new StackedColumn(1, 3, Map.of("Ivanov", 2, "Petrov", 1), null, null)
    );

    List<StackedColumn> expectedEdgeCaseTail = List.of(
        new StackedColumn(1, 3, Map.of("Ivanov", 2), null, null)
    );

    List<StackedColumn> expectedEdgeCaseBegin = List.of(
        new StackedColumn(1, 3, Map.of("Ivanov", 1, "Petrov", 1), null, null)
    );

    String lastName = "LASTNAME";
    GroupFunction groupFunction = GroupFunction.COUNT;

    List<StackedColumn> actualBlock = getStackedFilter(tProfile, lastName, groupFunction, 1, 3, null, null, null);
    assertEquals(expectedBlock, actualBlock);

    List<StackedColumn> actualEdgeCaseTail = getStackedFilter(tProfile, lastName, groupFunction, 1, 2, null, null, null);
    assertEquals(expectedEdgeCaseTail, actualEdgeCaseTail);

    List<StackedColumn> actualEdgeCaseBegin = getStackedFilter(tProfile, lastName, groupFunction, 2, 3, null, null, null);
    assertEquals(expectedEdgeCaseBegin, actualEdgeCaseBegin);
  }

  @Order(10)
  @Test
  public void getStacked()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked data using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_STACKED_USE_CASE_TABLE_NAME);
    GroupFunction groupFunction = GroupFunction.COUNT;
    long begin = 1;
    long end = 3;

    List<StackedColumn> actual = getStackedFilter(tProfile, "LASTNAME", groupFunction, begin, end, null, null, null);

    assertResults(actual, "Ivanov", 2, "Petrov", 1);
  }

  @Order(11)
  @Test
  public void getStackedCountFilter()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked data with filter using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_STACKED_FILTER_USE_CASE_TABLE_NAME);
    GroupFunction groupFunction = GroupFunction.COUNT;
    long begin = 1;
    long end = 3;

    List<StackedColumn> actualMsk = getStackedFilter(tProfile, "LASTNAME", groupFunction, begin, end,
                                               "CITY", new String[] {"Moscow"}, CompareFunction.EQUAL);
    assertResults(actualMsk, "Ivanov", 2);

    List<StackedColumn> actualEkb = getStackedFilter(tProfile, "LASTNAME", groupFunction, begin, end,
                                               "CITY", new String[] {"Yekaterinburg"}, CompareFunction.EQUAL);
    assertResults(actualEkb, "Petrov", 1);

    List<StackedColumn> actualLastNameNov = getStackedFilter(tProfile, "LASTNAME", groupFunction, begin, end,
                                               "LASTNAME", new String[] {"nov"}, CompareFunction.CONTAIN);
    List<StackedColumn> actualLastNameRov = getStackedFilter(tProfile, "LASTNAME", groupFunction, begin, end,
                                                    "LASTNAME", new String[] {"rov"}, CompareFunction.CONTAIN);

    assertResults(actualLastNameNov, "Ivanov", 2);
    assertResults(actualLastNameRov, "Petrov", 1);
  }

  @Order(24)
  @Test
  public void getStackedSumFilter() throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked sum data with filter using JDBC connection");

    TProfile tProfile = setupJdbcProfile("table_name_stacked_sum_filter_test");
    GroupFunction groupFunction = GroupFunction.SUM;
    long begin = 1;
    long end = 3;

    List<StackedColumn> actualMsk = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                               "CITY", new String[]{"Moscow"}, CompareFunction.EQUAL);
    assertEquals(3.0, actualMsk.get(0).getKeySum().get("HOUSE")); // Sum of houses in Moscow (1+2)

    List<StackedColumn> actualEkb = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                               "CITY", new String[]{"Yekaterinburg"}, CompareFunction.EQUAL);
    assertEquals(3.0, actualEkb.get(0).getKeySum().get("HOUSE")); // House value in Yekaterinburg (3)

    List<StackedColumn> actualLastNameOv = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                                      "LASTNAME", new String[]{"ov"}, CompareFunction.CONTAIN);
    assertEquals(6.0, actualLastNameOv.get(0).getKeySum().get("HOUSE")); // Total sum (1+2+3)
  }

  @Order(25)
  @Test
  public void getStackedAvgFilter() throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException {
    log.info("Use case: get stacked avg data with filter using JDBC connection");

    TProfile tProfile = setupJdbcProfile("table_name_stacked_avg_filter_test");
    GroupFunction groupFunction = GroupFunction.AVG;
    long begin = 1;
    long end = 3;

    List<StackedColumn> actualMsk = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                               "CITY", new String[]{"Moscow"}, CompareFunction.EQUAL);
    assertEquals(1.5, actualMsk.get(0).getKeyAvg().get("HOUSE")); // Avg of houses in Moscow (1,2) -> 1.5

    List<StackedColumn> actualEkb = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                               "CITY", new String[]{"Yekaterinburg"}, CompareFunction.EQUAL);
    assertEquals(3.0, actualEkb.get(0).getKeyAvg().get("HOUSE")); // House value in Yekaterinburg (3)

    List<StackedColumn> actualLastNameOv = getStackedFilter(tProfile, "HOUSE", groupFunction, begin, end,
                                                      "LASTNAME", new String[]{"ov"}, CompareFunction.CONTAIN);
    assertEquals(2.0, actualLastNameOv.get(0).getKeyAvg().get("HOUSE")); // Total avg (1+2+3)/3 = 2.0
  }

  private List<StackedColumn> getStackedFilter(TProfile tProfile,
                                         String columnName,
                                         GroupFunction groupFunction,
                                         long begin,
                                         long end,
                                         String filterColumnName,
                                         String[] filterData,
                                         CompareFunction compareFunction)
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    CProfile cProfile = getCProfileByName(tProfile, columnName);

    if (filterColumnName != null && filterData != null && compareFunction != null) {
      CProfile cProfileFilter = getCProfileByName(tProfile, filterColumnName);
      return configJdbc.getFStore().getStacked(
          tProfile.getTableName(),
          cProfile,
          groupFunction,
          cProfileFilter,
          filterData,
          compareFunction,
          begin,
          end
      );
    } else {
      return configJdbc.getFStore().getStacked(
          tProfile.getTableName(),
          cProfile,
          groupFunction,
          begin,
          end
      );
    }
  }

  @Order(12)
  @Test
  public void getGantt()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get gantt data using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_GANTT_USE_CASE_TABLE_NAME);
    CProfile firstGrpBy = getCProfileByName(tProfile, "LASTNAME");
    CProfile secondGrpBy = getCProfileByName(tProfile, "CITY");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<GanttColumnCount> expected = Arrays.asList(
        new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );

    List<GanttColumnCount> actual = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, begin, end);
    List<GanttColumnCount> actualParallel = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy, 2, begin, end);

    assertForGanttColumn(expected, actual);
    assertForGanttColumn(expected, actualParallel);
  }

  @Order(13)
  @Test
  public void getGanttFilter()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException,
      SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get gantt data with various filter combinations");

    // Setup test data and profiles
    TProfile tProfile = setupJdbcProfile(GET_GANTT_FILTER_USE_CASE_TABLE_NAME);
    CProfile firstGrpBy = getCProfileByName(tProfile, "LASTNAME");
    CProfile secondGrpBy = getCProfileByName(tProfile, "CITY");
    CProfile filterColumn = getCProfileByName(tProfile, "LASTNAME");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<GanttColumnCount> actualOv = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"ov"}, CompareFunction.CONTAIN, begin, end);
    List<GanttColumnCount> expectedOv = Arrays.asList(
        new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedOv, actualOv);

    List<GanttColumnCount> actualNov = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"nov"}, CompareFunction.CONTAIN, begin, end);
    List<GanttColumnCount> expectedNov = Arrays.asList(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedNov, actualNov);

    List<GanttColumnCount> actualExact = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"Ivanov"}, CompareFunction.EQUAL, begin, end);
    List<GanttColumnCount> expectedExact = Arrays.asList(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedExact, actualExact);

    List<GanttColumnCount> actualMulti = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"Ivanov", "Petrov"}, CompareFunction.EQUAL, begin, end);
    List<GanttColumnCount> expectedMulti = Arrays.asList(
        new GanttColumnCount("Petrov", Map.of("Yekaterinburg", 1)),
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedMulti, actualMulti);

    List<GanttColumnCount> actualNoMatch = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"xyz"}, CompareFunction.CONTAIN, begin, end);
    assertTrue(actualNoMatch.isEmpty());

    CProfile cityFilterColumn = getCProfileByName(tProfile, "CITY");
    List<GanttColumnCount> actualCityFilter = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        cityFilterColumn, new String[] {"Moscow"}, CompareFunction.EQUAL, begin, end);
    List<GanttColumnCount> expectedCityFilter = Arrays.asList(
        new GanttColumnCount("Ivanov", Map.of("Moscow", 2))
    );
    assertForGanttColumn(expectedCityFilter, actualCityFilter);

    List<GanttColumnCount> actualCaseSensitive = configJdbc.getFStore().getGantt(
        tProfile.getTableName(), firstGrpBy, secondGrpBy,
        filterColumn, new String[] {"ivanov"}, CompareFunction.EQUAL, begin, end);
    assertTrue(actualCaseSensitive.isEmpty());
  }

  @Order(14)
  @Test
  public void getGanttSum()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException,
      SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get gantt sum data grouped by lastname and firstname");

    TProfile tProfile = setupJdbcProfile(GET_GANTT_FILTER_USE_CASE_TABLE_NAME);
    CProfile lastNameGrpBy = getCProfileByName(tProfile, "LASTNAME");
    CProfile firstNameGrpBy = getCProfileByName(tProfile, "FIRSTNAME");
    CProfile houseSumColumn = getCProfileByName(tProfile, "HOUSE");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    // Test 1: Group by LASTNAME, sum HOUSE
    List<GanttColumnSum> actualLastName = configJdbc.getFStore().getGanttSum(
        tProfile.getTableName(), lastNameGrpBy, houseSumColumn, begin, end);

    List<GanttColumnSum> expectedLastName = Arrays.asList(
        new GanttColumnSum("Petrov", 3.0),
        new GanttColumnSum("Ivanov", 3.0)
    );
    assertGanttSumColumnsEqual(expectedLastName, actualLastName);

    // Test 2: Group by FIRSTNAME, sum HOUSE
    List<GanttColumnSum> actualFirstName = configJdbc.getFStore().getGanttSum(
        tProfile.getTableName(), firstNameGrpBy, houseSumColumn, begin, end);

    List<GanttColumnSum> expectedFirstName = Arrays.asList(
        new GanttColumnSum("Alex", 1.0),
        new GanttColumnSum("Ivan", 2.0),
        new GanttColumnSum("Oleg", 3.0)
    );
    assertGanttSumColumnsEqual(expectedFirstName, actualFirstName);

    // Test 3: Filtered time range (begin=2, end=2)
    List<GanttColumnSum> actualFiltered = configJdbc.getFStore().getGanttSum(
        tProfile.getTableName(), lastNameGrpBy, houseSumColumn, 2, 2);

    List<GanttColumnSum> expectedFiltered = Arrays.asList(
        new GanttColumnSum("Ivanov", 2.0)
    );
    assertGanttSumColumnsEqual(expectedFiltered, actualFiltered);
  }

  private void assertGanttSumColumnsEqual(List<GanttColumnSum> expected, List<GanttColumnSum> actual) {
    assertEquals(expected.size(), actual.size(), "Number of result rows mismatch");

    Map<String, Double> expectedMap = expected.stream()
        .collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));

    Map<String, Double> actualMap = actual.stream()
        .collect(Collectors.toMap(GanttColumnSum::getKey, GanttColumnSum::getValue));

    assertEquals(expectedMap, actualMap, "Result content mismatch");
  }

  @Order(15)
  @Test
  public void getDistinct()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException {
    log.info("Use case: get distinct data values");

    TProfile tProfile = setupJdbcProfile(GET_DISTINCT_USE_CASE_TABLE_NAME);
    CProfile cProfileFirstName = getCProfileByName(tProfile, "FIRSTNAME");
    CProfile cProfileLastName = getCProfileByName(tProfile, "LASTNAME");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<String> actualFirstNameDesc = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.DESC, 10, begin, end);
    List<String> actualLastNameDesc = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.DESC, 10, begin, end);

    List<String> actualFirstNameAsc = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, 10, begin, end);
    List<String> actualLastNameAsc = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.ASC, 10, begin, end);

    List<String> actualFirstNameAscLimitTwo = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileFirstName, OrderBy.ASC, 2, begin, end);
    List<String> actualLastNameAscLimitOne = configJdbc.getFStore()
        .getDistinct(tProfile.getTableName(), cProfileLastName, OrderBy.ASC, 1, begin, end);

    assertEquals(List.of("Oleg", "Ivan", "Alex"), actualFirstNameDesc);
    assertEquals(List.of("Petrov", "Ivanov"), actualLastNameDesc);

    assertEquals(List.of("Alex", "Ivan", "Oleg"), actualFirstNameAsc);
    assertEquals(List.of("Ivanov", "Petrov"), actualLastNameAsc);

    assertEquals(List.of("Alex", "Ivan"), actualFirstNameAscLimitTwo);
    assertEquals(List.of("Ivanov"), actualLastNameAscLimitOne);
  }

  @Order(15)
  @Test
  public void getDistinctWithFilter()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException {
    log.info("Use case: get distinct data values with filtering");

    TProfile tProfile = setupJdbcProfile("table_name_distinct_filter_test");
    CProfile cProfileFirstName = getCProfileByName(tProfile, "FIRSTNAME");
    CProfile cProfileLastName = getCProfileByName(tProfile, "LASTNAME");
    CProfile cProfileCity = getCProfileByName(tProfile, "CITY");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<String> actualMoscow = configJdbc.getFStore().getDistinct(
        tProfile.getTableName(),
        cProfileFirstName,
        OrderBy.ASC,
        10,
        begin,
        end,
        cProfileCity,
        new String[] {"Moscow"},
        CompareFunction.EQUAL
    );
    assertEquals(List.of("Alex", "Ivan"), actualMoscow);

    List<String> actualContainOv = configJdbc.getFStore().getDistinct(
        tProfile.getTableName(),
        cProfileFirstName,
        OrderBy.ASC,
        10,
        begin,
        end,
        cProfileLastName,
        new String[] {"ov"},
        CompareFunction.CONTAIN
    );
    assertEquals(List.of("Alex", "Ivan", "Oleg"), actualContainOv);

    List<String> actualLimit = configJdbc.getFStore().getDistinct(
        tProfile.getTableName(),
        cProfileLastName,
        OrderBy.ASC,
        1,
        begin,
        end,
        cProfileFirstName,
        new String[] {"Ivan"},
        CompareFunction.EQUAL
    );
    assertEquals(List.of("Ivanov"), actualLimit);

    List<String> actualNoMatch = configJdbc.getFStore().getDistinct(
        tProfile.getTableName(),
        cProfileFirstName,
        OrderBy.ASC,
        10,
        begin,
        end,
        cProfileCity,
        new String[] {"London"},
        CompareFunction.EQUAL
    );
    assertTrue(actualNoMatch.isEmpty());

    List<String> actualMultiFilter = configJdbc.getFStore().getDistinct(
        tProfile.getTableName(),
        cProfileCity,
        OrderBy.DESC,
        10,
        begin,
        end,
        cProfileFirstName,
        new String[] {"Alex", "Oleg"},
        CompareFunction.EQUAL
    );

    assertEquals(new HashSet<>(List.of("Yekaterinburg", "Moscow")), new HashSet<>(actualMultiFilter));
  }

  @Order(16)
  @Test
  public void getRawDataAll()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get raw data using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_RAW_USE_CASE_TABLE_NAME);

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    assertRawDataJdbc(tProfile.getTableName(), 1, 3);
  }

  @Order(17)
  @Test
  public void getRawDataAllFilter()
      throws SQLException, TableNameEmptyException {
    log.info("Use case: get raw data with filter using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_RAW_FILTER_USE_CASE_TABLE_NAME);

    CProfile cProfileFilter = getCProfileByName(tProfile, "FIRSTNAME");
    String filter = "Alex";

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<List<Object>> actual = configJdbc.getFStore().getRawDataAll(tProfile.getTableName(), cProfileFilter, filter, 1, 3);
    List<List<Object>> expected = new ArrayList<>();
    expected.add(Arrays.asList(new String[]{"1", "Alex", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));

    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  @Order(18)
  @Test
  public void getRawDataByColumn()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get raw data for column using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_RAW_COLUMN_USE_CASE_TABLE_NAME);

    CProfile cProfile = getCProfileByName(tProfile, "FIRSTNAME");

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<List<Object>> actual = configJdbc.getFStore().getRawDataByColumn(tProfile.getTableName(), cProfile, 1, 3);
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);
    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(1))).collect(Collectors.toList()), actual);
  }

  @Order(19)
  @Test
  public void getBatchResultSetRegularTable() throws TableNameEmptyException, SqlColMetadataException, IOException {
    log.info("Use case: get raw data using BatchResultSet for regular table with CSV");

    String fileName = new File("").getAbsolutePath() + FILE_SEPARATOR +
        Paths.get("src", "test", "resources", "csv", "file.csv");

    String csvSplitBy = ",";

    SProfile sProfile = getSProfileCsv(GET_RESULT_SET_REGULAR_USE_CASE_TABLE_NAME, fileName, csvSplitBy, TType.REGULAR, IType.GLOBAL, BType.BERKLEYDB, false);

    TProfile tProfile = configCsv.getFStore().loadCsvTableMetadata(fileName, csvSplitBy, sProfile);

    configCsv.getFStore().putDataCsvBatch(tProfile.getTableName(), fileName, csvSplitBy, 2);

    assertDataCsvBatchTest(configCsv.getFStore(), sProfile, fileName, false, 10, false);
  }

  @Order(20)
  @Test
  public void getBatchResultSetTimeSeriesTable()
      throws SQLException, TableNameEmptyException {
    log.info("Use case: get raw data using BatchResultSet for time series tables with JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_RESULT_SET_TIME_SERIES_USE_CASE_TABLE_NAME);

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    BatchResultSet batchResultSet = configJdbc.getFStore().getBatchResultSet(tProfile.getTableName(), 1, 3, 10);

    List<List<Object>> actual = new ArrayList<>();
    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      log.info("Output by fetchSize: " + var);

      actual.addAll(var);
    }
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);

    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  @Order(21)
  @Test
  public void getFirst()
      throws SQLException, TableNameEmptyException {
    log.info("Use case: get stacked data using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_FIRST_TIMESTAMP_USE_CASE_TABLE_NAME);
    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");
    long begin = Long.MIN_VALUE;
    long end = Long.MAX_VALUE;

    long first = configJdbc.getFStore().getFirst(tProfile.getTableName(), begin, end);

    assertEquals(1L, first);
  }

  @Order(22)
  @Test
  public void getLast() throws SQLException, TableNameEmptyException {
    log.info("Use case: get stacked data using JDBC connection");

    TProfile tProfile = setupJdbcProfile(GET_LAST_TIMESTAMP_USE_CASE_TABLE_NAME);
    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");
    long begin = Long.MIN_VALUE;
    long end = Long.MAX_VALUE;

    long last = configJdbc.getFStore().getLast(tProfile.getTableName(), begin, end);

    assertEquals(3L, last);
  }

  @Order(23)
  @Test
  public void getGanttSumFiltered()
      throws SQLException, TableNameEmptyException, BeginEndWrongOrderException,
      SqlColMetadataException, GanttColumnNotSupportedException {
    log.info("Use case: get filtered gantt sum data");

    TProfile tProfile = setupJdbcProfile("table_name_gantt_sum_filtered_test");
    CProfile lastNameGrpBy = getCProfileByName(tProfile, "LASTNAME");
    CProfile houseSumColumn = getCProfileByName(tProfile, "HOUSE");
    CProfile firstNameFilter = getCProfileByName(tProfile, "FIRSTNAME");
    long begin = 1;
    long end = 3;

    putDataJdbc(configJdbc.getFStore(), tProfile, configJdbc.getH2DbConnection(), "SELECT * FROM person");

    List<GanttColumnSum> actualIvan = configJdbc.getFStore().getGanttSum(
        tProfile.getTableName(),
        lastNameGrpBy,
        houseSumColumn,
        firstNameFilter,
        new String[]{"Ivan"},
        CompareFunction.EQUAL,
        begin,
        end
    );

    List<GanttColumnSum> expectedIvan = List.of(
        new GanttColumnSum("Ivanov", 2.0)
    );

    assertGanttSumColumnsEqual(expectedIvan, actualIvan);

    List<GanttColumnSum> actualContainE = configJdbc.getFStore().getGanttSum(
        tProfile.getTableName(),
        lastNameGrpBy,
        houseSumColumn,
        firstNameFilter,
        new String[]{"e"},
        CompareFunction.CONTAIN,
        begin,
        end
    );

    List<GanttColumnSum> expectedContainE = Arrays.asList(
        new GanttColumnSum("Ivanov", 1.0),
        new GanttColumnSum("Petrov", 3.0)
    );

    assertGanttSumColumnsEqual(expectedContainE, actualContainE);
  }

  protected void assertForGanttColumn(List<GanttColumnCount> expected, List<GanttColumnCount> actual) {
    expected.forEach(e ->
                         assertEquals(e.getGantt(), actual.stream()
                             .filter(f -> f.getKey().equals(e.getKey()))
                             .findAny()
                             .orElseThrow()
                             .getGantt()));
  }

  private void testBlockKeyTail(String tableName, long begin, long end) throws Exception {
    List<BlockKeyTail> result = configJdbc.getFStore().getBlockKeyTailList(tableName, begin, end);

    Assertions.assertEquals(1, result.size(), "The result size should be 1.");
    Assertions.assertEquals(begin, result.getFirst().getKey(), "The key should match the begin value.");
    Assertions.assertEquals(end, result.getLast().getTail(), "The tail should match the end value.");
  }

  private void assertResults(List<StackedColumn> actual, String firstValue, int expectedFirstValueCount) throws AssertionError {
    assertNotNull(actual);
    assertEquals(1, actual.size());
    assertEquals(firstListStackedValue(actual, firstValue), expectedFirstValueCount);
  }

  private void assertResults(List<StackedColumn> actual, String firstValue, int expectedFirstValueCount, String secondValue, int expectedSecondValueCount) throws AssertionError {
    assertResults(actual, firstValue, expectedFirstValueCount);
    assertEquals(firstListStackedValue(actual, secondValue), expectedSecondValueCount);
  }

  public Object firstListStackedValue(List<StackedColumn> list, String key) {
    return list.stream()
        .map(column -> column.getKeyCount().get(key))
        .filter(Objects::nonNull)
        .mapToInt(Integer::intValue)
        .sum();
  }

  private TProfile setupJdbcProfile(String tableName) throws SQLException, TableNameEmptyException {
    SProfile sProfile = getSProfileJdbc(tableName, TType.TIME_SERIES, IType.LOCAL, BType.BERKLEYDB, true);
    sProfile.getCsTypeMap()
        .put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    return configJdbc.getFStore().loadJdbcTableMetadata(configJdbc.getH2DbConnection(), select, sProfile);
  }

  private void assertData(String actualTableName,
                          List<List<Object>> expectedData) {
    List<List<Object>> actualData = configDirect.getFStore()
        .getRawDataAll(actualTableName, Long.MIN_VALUE, Long.MAX_VALUE);
    assertEquals(expectedData.toString(), actualData.toString());
    assertEquals(expectedData.size(), actualData.size());
  }

  private void assertRawDataJdbc(String tableName,
                                 long begin,
                                 long end) {
    List<List<Object>> actual = configJdbc.getFStore().getRawDataAll(tableName, begin, end);
    List<List<Object>> expected = new ArrayList<>();
    loadExpected(expected);
    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  private void loadDataTablePersonH2(H2Database h2Db) throws SQLException {
    String tableName = "PERSON";
    String createTableSql = String.format("""
                                              CREATE TABLE %s (
                                                id        INT,
                                                firstname VARCHAR(64),
                                                lastname  VARCHAR(64),
                                                house     INT,
                                                city      VARCHAR(64),
                                                birthday  TIMESTAMP
                                              )
                                              """, tableName);

    h2Db.execute(createTableSql);

    LocalDateTime birthday = LocalDateTime.of(2023, 1, 1, 1, 1, 1);
    h2Db.insert(Person.builder().id(1).firstname("Alex").lastname("Ivanov").house(1).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(2).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").birthday(birthday).build());
    h2Db.insert(Person.builder().id(3).firstname("Oleg").lastname("Petrov").house(3).city("Yekaterinburg").birthday(birthday).build());
  }

  protected void loadExpected(List<List<Object>> expected) {
    expected.add(Arrays.asList(new String[]{"1", "Alex", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"2", "Ivan", "Ivanov", "2", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"3", "Oleg", "Petrov", "3", "Yekaterinburg", "01.01.2023 01:01:01"}));
  }

  protected void assertForRaw(List<List<Object>> expected,
                              List<List<Object>> actual) {
    for (int i = 0; i < expected.size(); i++) {
      for (int j = 0; j < expected.get(i).size(); j++) {
        assertEquals(String.valueOf(expected.get(i).get(j)), String.valueOf(actual.get(i).get(j)));
      }
    }
  }

  private CProfile getCProfileByName(TProfile tProfile,
                                     String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findAny().orElseThrow();
  }

  private static File createSubDirWithPostfix(File rootDir, String subDirName) {
    File subDir = new File(rootDir, subDirName);
    if (!subDir.mkdir()) {
      throw new RuntimeException("Failed to create subdirectory: " + subDirName);
    }

    return subDir;
  }

  @AfterAll
  public void closeDb() {
    configDirect.getBerkleyDB().closeDatabase();
    configJdbc.getBerkleyDB().closeDatabase();
    configCsv.getBerkleyDB().closeDatabase();
  }
}
