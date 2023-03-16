package org.fbase.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.util.Assert;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.IType;
import org.fbase.source.ClickHouse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseCHQueryDataGlobalTest implements ClickHouse {

  private FStore fStore;
  private TProfile tProfile;
  private List<CProfile> cProfiles;

  private BerkleyDB berkleyDB;

  private ObjectMapper objectMapper;

  @BeforeAll
  public void initialLoading() throws IOException {
    String dbFolder = getTestDbFolder("C:\\Users\\.temp", "clickhouse_test");

    this.berkleyDB = new BerkleyDB(dbFolder, false);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbFolder).setBlockSize(16);
    FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    try {
      tProfile = fBase.getFStore().getTProfile(tableName);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
    cProfiles = tProfile.getCProfiles();

    objectMapper = new ObjectMapper();
  }

  @Test
  public void getGanttRawRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT pickup_cdeligibil, vendor_id, COUNT(vendor_id)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY pickup_cdeligibil, vendor_id;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("pickup_cdeligibil__vendor_id.json");
    List<GanttColumn> actual = getGanttDataActual("PICKUP_CDELIGIBIL", "VENDOR_ID");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttEnumEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             select dropoff_puma, dropoff_borocode, count(dropoff_borocode)
             from datasets.trips_mergetree
             where toyear(pickup_date) = 2016
             group by dropoff_puma, dropoff_borocode;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("dropoff_puma__dropoff_borocode.json");
    List<GanttColumn> actual = getGanttDataActual("DROPOFF_PUMA", "DROPOFF_BOROCODE");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttHistHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
                  SELECT trip_type, pickup_boroname, COUNT(pickup_boroname)
                  FROM datasets.trips_mergetree
                  WHERE toYear(pickup_date) = 2016
                  GROUP BY trip_type, pickup_boroname;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("trip_type__pickup_boroname.json");
    List<GanttColumn> actual = getGanttDataActual("TRIP_TYPE", "PICKUP_BORONAME");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttHistRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT trip_type, vendor_id, COUNT(vendor_id)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY trip_type, vendor_id;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("trip_type__vendor_id.json");
    List<GanttColumn> actual = getGanttDataActual("TRIP_TYPE", "VENDOR_ID");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttHistEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT trip_type, dropoff_boroname, COUNT(dropoff_boroname)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY trip_type, dropoff_boroname;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("trip_type__dropoff_boroname.json");
    List<GanttColumn> actual = getGanttDataActual("TRIP_TYPE", "DROPOFF_BORONAME");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttEnumRaw()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT dropoff_boroname, vendor_id, COUNT(vendor_id)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY dropoff_boroname, vendor_id;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("dropoff_boroname__vendor_id.json");
    List<GanttColumn> actual = getGanttDataActual("DROPOFF_BORONAME", "VENDOR_ID");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttEnumHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT dropoff_boroname, pickup_boroname, COUNT(pickup_boroname)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY dropoff_boroname, pickup_boroname;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("dropoff_boroname__pickup_boroname.json");
    List<GanttColumn> actual = getGanttDataActual("DROPOFF_BORONAME", "PICKUP_BORONAME");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttRawHist()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT pickup_cdeligibil, pickup_boroname, COUNT(pickup_boroname)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY pickup_cdeligibil, pickup_boroname;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("pickup_cdeligibil__pickup_boroname.json");
    List<GanttColumn> actual = getGanttDataActual("PICKUP_CDELIGIBIL", "PICKUP_BORONAME");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getGanttRawEnum()
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException, IOException {
    String query = """
             SELECT pickup_cdeligibil, cab_type, COUNT(cab_type)
             FROM datasets.trips_mergetree
             WHERE toYear(pickup_date) = 2016
             GROUP BY pickup_cdeligibil, cab_type;
        """;
    log.info("Query: " + "\n" + query);

    List<GanttColumn> expected = getGanttDataExpected("pickup_cdeligibil__cab_type.json");
    List<GanttColumn> actual = getGanttDataActual("PICKUP_CDELIGIBIL", "CAB_TYPE");

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertGanttListEquals(expected, actual);
    assertGanttMapEquals(expected, actual);
  }

  @Test
  public void getStackedHist() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    List<StackedColumn> expected = getStackedDataExpected("trip_type.json");
    List<StackedColumn> actual = getListStackedColumnActual("TRIP_TYPE", Long.MIN_VALUE, Long.MAX_VALUE);

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

  private void assertGanttMapEquals(List<GanttColumn> expected, List<GanttColumn> actual) {
    expected.forEach(exp -> Assert.equals(exp.getGantt(), actual.stream()
        .filter(f -> f.getKey().equalsIgnoreCase(exp.getKey()))
        .findFirst()
        .orElseThrow()
        .getGantt()));
  }

  private void assertStackedMapEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    expected.forEach(exp -> Assert.equals(exp.getKeyCount(), actual.stream()
        .findFirst()
        .orElseThrow()
        .getKeyCount()));
  }

  public void assertGanttListEquals(List<GanttColumn> expected, List<GanttColumn> actual) {
    assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
  }

  public void assertStackedListEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    assertEquals(expected.size(), actual.size());
  }

  private List<GanttColumn> getGanttDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getGanttTestData(fileName), new TypeReference<>() {});
  }

  private List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStackedTestData(fileName), new TypeReference<>() {});
  }

  private List<GanttColumn> getGanttDataActual(String firstColName, String secondColName)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return getGanttDataActual(firstColName, secondColName, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private List<GanttColumn> getGanttDataActual(String firstColName, String secondColName, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();
    return getListGanttColumnTwoLevelGrouping(fStore, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  private List<GanttColumn> getListGanttColumnTwoLevelGrouping(FStore fStore,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return fStore.getGColumnListTwoLevelGroupBy(tProfile.getTableName(), IType.GLOBAL, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  private List<StackedColumn> getListStackedColumnActual(String firstColName, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    return getListStackedColumn(fStore, cProfile, begin, end);
  }

  private List<StackedColumn> getListStackedColumn(FStore fStore,
      CProfile cProfile, long begin, long end) throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, begin, end);
  }

  private String getGanttTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src","test", "resources", "json", "gantt", fileName));
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src","test", "resources", "json", "stacked", fileName));
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();
  }
}
