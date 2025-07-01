package org.fbase.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
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
public class FBaseCHQueryStackedTest implements ClickHouse {

  private FStore fStore;
  private TProfile tProfile;
  private List<CProfile> cProfiles;

  private BerkleyDB berkleyDB;

  private ObjectMapper objectMapper;

  @BeforeAll
  public void initialLoading() throws IOException {
    String dbFolder = getTestDbFolder();

    this.berkleyDB = new BerkleyDB(dbFolder, false);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbFolder);
    FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    try {
      tProfile = fBase.getFStore().getTProfile(tableName);
      log.info(tProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
    cProfiles = tProfile.getCProfiles();

    objectMapper = new ObjectMapper();
  }

  @Test
  public void stackedColumnTripTypeCountGroupByTest() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");

    List<StackedColumn> expected = getStackedDataExpected("trip_type.json");
    List<StackedColumn> actual = fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, Long.MIN_VALUE, Long.MAX_VALUE);

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
        actualResult.stream()
            .findAny()
            .orElseThrow()
            .getKeyCount()
            .putIfAbsent(s, Math.toIntExact(batch.map(IntSummaryStatistics::getSum).orElseThrow()));
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
  public void stackedColumnTripTypeCountGroupByBugWrongResultTest() throws BeginEndWrongOrderException, SqlColMetadataException, IOException {
    String query = """
        SELECT trip_type, COUNT(trip_type)
        FROM datasets.trips_mergetree
        WHERE toYear(pickup_date) = 2016
        GROUP BY trip_type;
        """;
    log.info("Query: " + "\n" + query);

    CProfile cProfile = getCProfileByName("TRIP_TYPE");

    long begin = getUnixTimestampNoOffset(LocalDateTime.of(2016, 1, 1, 0, 0, 0, 0));
    long end = getUnixTimestampNoOffset(LocalDateTime.of(2016, 12, 31, 23, 59, 59, 999999999));

    List<StackedColumn> actual = fStore.getStacked(tProfile.getTableName(), cProfile, GroupFunction.COUNT, begin, end);

    Map<String, IntSummaryStatistics> batchData = actual.stream()
        .toList()
        .stream()
        .map(StackedColumn::getKeyCount)
        .flatMap(sc -> sc.entrySet().stream())
        .collect(Collectors.groupingBy(entry -> Objects.requireNonNullElse(entry.getKey(), ""),
                                       Collectors.summarizingInt(Map.Entry::getValue)));

    System.out.println(batchData);

    // TODO Wrong result while setting begin/end explicitly
    //assertData("trip_type.json", actual);
  }

  protected long getUnixTimestamp(LocalDateTime localDateTime) {
    ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
    return localDateTime.toInstant(offset).toEpochMilli();
  }

  protected long getUnixTimestampNoOffset(LocalDateTime localDateTime) {
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findAny().orElseThrow();
  }

  private void assertStackedMapEquals(List<StackedColumn> expected, List<StackedColumn> actual) {
    expected.forEach(exp -> assertEquals(exp.getKeyCount(), actual.stream()
        .findFirst()
        .orElseThrow()
        .getKeyCount()));
  }

  public void assertStackedListEquals(List<StackedColumn> expected,
                                      List<StackedColumn> actual) {
    assertEquals(expected.stream().findAny().orElseThrow().getKeyCount(),
                 actual.stream().findAny().orElseThrow().getKeyCount());
  }

  private void assertData(String expectedJsonFile, List<StackedColumn> actual) throws IOException {
    List<StackedColumn> expected = getStackedDataExpected(expectedJsonFile);

    log.info("Expected: " + expected);
    log.info("Actual: " + actual);

    assertStackedListEquals(expected, actual);
  }

  protected List<StackedColumn> getStackedDataExpected(String fileName) throws IOException {
    return objectMapper.readValue(getStackedTestData(fileName), new TypeReference<>() {
    });
  }

  private String getStackedTestData(String fileName) throws IOException {
    return Files.readString(Paths.get("src", "test", "resources", "json", "stacked", fileName));
  }

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();
  }
}
