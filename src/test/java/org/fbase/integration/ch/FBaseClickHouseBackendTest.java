package org.fbase.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.common.AbstractBackendSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumn;
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
public class FBaseClickHouseBackendTest extends AbstractBackendSQLTest {

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
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, null, null);

    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = fStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void stackedColumnTripTypeTest() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("TRIP_TYPE");

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actual = fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, GroupFunction.COUNT, timestamps[0], timestamps[1]);

    assertData("trip_type.json", actual);
  }

  @Test
  public void stackedColumnTripIdTest() throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("PASSENGER_COUNT");

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actual = fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, GroupFunction.COUNT, timestamps[0], timestamps[1]);

    assertData("passenger_count.json", actual);
  }

  @Test
  public void stackedColumnFilterTripIdTest()
      throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    CProfile cProfileFilter = getCProfileByName("TRIP_ID");
    String filter = "36552792";

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actual =
        fStore.getSColumnListByCProfileFilter(tProfile.getTableName(), cProfile, GroupFunction.COUNT, cProfileFilter, filter, timestamps[0], timestamps[1]);

    assertData("trip_type_filter_trip_id.json", actual);
  }

  @Test
  public void stackedColumnFilterVendorIdTest()
      throws IOException, BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("TRIP_TYPE");
    CProfile cProfileFilter = getCProfileByName("VENDOR_ID");
    String filter = "1";

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actual =
        fStore.getSColumnListByCProfileFilter(tProfile.getTableName(), cProfile, GroupFunction.COUNT, cProfileFilter, filter, timestamps[0], timestamps[1]);

    assertData("trip_type_filter_vendor_id.json", actual);
  }

  @Test
  public void stackedColumnVendorIdTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("TRIP_ID");

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actualSum =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, GroupFunction.SUM, timestamps[0], timestamps[1]);
    List<StackedColumn> actualAvg =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, GroupFunction.AVG, timestamps[0], timestamps[1]);

    assertEquals(85054314756844435D,
                 actualSum.stream().findAny().orElseThrow().getKeySum().get(cProfile.getColName().toLowerCase()));
    assertEquals(1084536854.2993798D,
                 actualAvg.stream().findAny().orElseThrow().getKeyAvg().get(cProfile.getColName().toLowerCase()));
  }

  @Test
  public void getLastTimestampTest() {
    long[] timestamps = getBeginEndTimestamps();

    long actual = fStore.getLastTimestamp(tProfile.getTableName(), timestamps[0], timestamps[1]);

    LocalDate expectedDate = LocalDate.of(2016, 6, 30);
    LocalDate actualDate = LocalDate.ofInstant(Instant.ofEpochMilli(actual * 1000), ZoneId.systemDefault());

    assertEquals(1467244800, actual);
    assertEquals(expectedDate, actualDate);
  }

  @Test
  public void ganttColumnPumaBoroCodeTest()
      throws IOException, BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    CProfile firstGrpBy = getCProfileByName("DROPOFF_PUMA");
    CProfile secondGrpBy = getCProfileByName("DROPOFF_BOROCODE");

    List<GanttColumn> expected = getGanttDataExpected("dropoff_puma__dropoff_borocode.json");

    long[] timestamps = getBeginEndTimestamps();
    List<GanttColumn> actual =
        fStore.getGColumnListTwoLevelGroupBy(tProfile.getTableName(), firstGrpBy, secondGrpBy, timestamps[0], timestamps[1]);

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  protected long[] getBeginEndTimestamps() {
    long begin = getUnitTimestamp(LocalDateTime.of(2016, 1, 1, 0, 0, 0, 0));
    long end = getUnitTimestamp(LocalDateTime.of(2016, 12, 31, 23, 59, 59, 999999999));
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
}
