package org.fbase.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.common.AbstractBackendSQLTest;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseClickHouseSQLStackedTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:clickhouse://localhost:8123";
  private final String driverClassName = "com.clickhouse.jdbc.ClickHouseDriver";
  private final String tableName = "datasets.trips_mergetree";
  private final String tsName = "PICKUP_DATE";
  private final String select = "select * from " + tableName + " limit 1";

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.CLICKHOUSE;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "admin", "admin");

    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = fStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void stackedColumnTripTypeCountGroupBy1Test() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    String query = """
        SELECT COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY trip_type;
        """;
    log.info("Query COUNT with GROUP BY: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> actual = fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, timestamps[0], timestamps[1]);

    assertData("trip_type.json", actual);
  }

  @Test
  public void stackedColumnTripTypeCountGroupBy2Test() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> expected = getStackedDataExpected("trip_type.json");
    List<StackedColumn> actual = getListStackedColumnActual(tProfile.getCProfiles(), cProfile.getColName(), timestamps[0], timestamps[1]);

    StackedColumn stackedColumn = new StackedColumn();
    stackedColumn.setKey(0);
    stackedColumn.setTail(0);
    List<StackedColumn> actualResult = new ArrayList<>();
    actualResult.add(stackedColumn);

    Set<String> series = new LinkedHashSet<>();
    actual.stream()
        .map(StackedColumn::getKeyCount)
        .map(Map::keySet)
        .flatMap(Collection::stream)
        .forEach(series::add);

    Map<String, IntSummaryStatistics> batchDataLocal = actual.stream()
        .toList()
        .stream()
        .map(StackedColumn::getKeyCount)
        .flatMap(sc -> sc.entrySet().stream())
        .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summarizingInt(Map.Entry::getValue)));

    series.forEach(s -> {
      Optional<IntSummaryStatistics> batch = Optional.ofNullable(batchDataLocal.get(s));
      try {
        actualResult.stream().findAny().get().getKeyCount().putIfAbsent(s,
                                                                        Math.toIntExact(batch.map(IntSummaryStatistics::getSum).orElseThrow()));
      } catch (Exception exception) {
        log.info(exception);
      }
    });

    log.info("Expected: " + expected);
    log.info("Actual: " + actualResult);

    assertStackedListEquals(expected, actualResult);
    assertStackedMapEquals(expected, actualResult);
  }

  @Test
  public void stackedColumnPassengerCountTest() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    String query = """
        SELECT passenger_count, COUNT(passenger_count)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY passenger_count;
        """;
    log.info("Query COUNT with GROUP BY: " + "\n" + query);

    CProfile cProfile = getCProfileByName("PASSENGER_COUNT");

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> actual = fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, timestamps[0], timestamps[1]);

    assertData("passenger_count.json", actual);
  }

  @Test
  public void stackedColumnTripTypeFilterTripIdTest() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016 and trip_id = 36552792
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    CProfile cProfileFilter = getCProfileByName("TRIP_ID");
    String filter = "36552792";

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> actual =
        fStore.getStacked(tProfile.getTableName(),
                          cProfile,
                          GroupFunction.COUNT,
                          cProfileFilter,
                          new String[]{filter},
                          CompareFunction.EQUAL,
                          timestamps[0],
                          timestamps[0]);

    assertData("trip_type_filter_trip_id.json", actual);
  }

  @Test
  public void stackedColumnTripTypeFilterVendorIdTest() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016 and vendor_id = 1
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    CProfile cProfileFilter = getCProfileByName("VENDOR_ID");
    String filter = "1";

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> actual =
        fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT,
                          cProfileFilter, new String[]{filter}, CompareFunction.EQUAL,
                          timestamps[0], timestamps[1]);

    assertData("trip_type_filter_vendor_id.json", actual);
  }

  @Test
  public void stackedColumnTripIdSumTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    String querySum = """
        SELECT SUM(trip_id)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016;
        """;
    log.info("Query SUM: " + "\n" + querySum);

    String queryAvg = """
        SELECT AVG(trip_id)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016;
        """;
    log.info("Query AVG: " + "\n" + queryAvg);

    CProfile cProfile = getCProfileByName("TRIP_ID");

    long[] timestamps = getUnixBeginEndTimestamps();
    List<StackedColumn> actualSum =
        fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.SUM, timestamps[0], timestamps[1]);
    List<StackedColumn> actualAvg =
        fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.AVG, timestamps[0], timestamps[1]);

    assertEquals(85054314756844435D,
                 actualSum.stream().findAny().orElseThrow().getKeySum().get(cProfile.getColName().toLowerCase()));
    assertEquals(1084536854.2993798D,
                 actualAvg.stream().findAny().orElseThrow().getKeyAvg().get(cProfile.getColName().toLowerCase()));
  }

  @Test
  public void stackedColumnCheckLastTimestampTest() {
    long[] timestamps = getUnixBeginEndTimestamps();

    long actual = fStore.getLast(tProfile.getTableName(), timestamps[0], timestamps[1]);

    LocalDate expectedDate = LocalDate.of(2016, 6, 30);
    LocalDate actualDate = LocalDate.ofInstant(Instant.ofEpochMilli(actual), ZoneId.systemDefault());

    assertEquals(1467244800000L, actual);
    assertEquals(expectedDate, actualDate);
  }

  protected long[] getUnixBeginEndTimestamps() {
    long begin = getUnixTimestamp(LocalDateTime.of(2016, 1, 1, 0, 0, 0, 0));
    long end = getUnixTimestamp(LocalDateTime.of(2016, 12, 31, 23, 59, 59, 999999999));
    return new long[]{begin, end};
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findAny().orElseThrow();
  }

  private void assertData(String expectedJsonFile, List<StackedColumn> actual) throws IOException {
    List<StackedColumn> expected = getStackedDataExpected(expectedJsonFile);

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertStackedListEquals(expected, actual);
  }

  private List<StackedColumn> getListStackedColumnActual(List<CProfile> cProfiles, String firstColName, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    return getListStackedColumn(fStore, cProfile, begin, end);
  }

  private void assertStackedMapEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    expected.forEach(exp -> assertEquals(exp.getKeyCount(), actual.stream()
        .findFirst()
        .orElseThrow()
        .getKeyCount()));
  }

  public void assertStackedListEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    assertEquals(expected.size(), actual.size());
  }

  private List<StackedColumn> getListStackedColumn(FStore fStore,
                                                   CProfile cProfile, long begin, long end) throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, begin, end);
  }
}
