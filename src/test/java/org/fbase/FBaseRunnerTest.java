package org.fbase;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.backend.BerkleyDB;
import org.fbase.common.AbstractH2Test;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.BdbAndFBase;
import org.fbase.model.BeginAndEnd;
import org.fbase.model.FSColAndRange;
import org.fbase.model.FSColumn;
import org.fbase.model.FilterColumn;
import org.fbase.model.LoadDataType;
import org.fbase.model.PermutationState;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Log4j2
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FBaseRunnerTest extends AbstractH2Test {

  @Order(1)
  @Test
  public void testPermutationNone() {
    testInParallel(PermutationState.NONE);
  }

  @Order(2)
  @Test
  public void testPermutationPartial() {
    testInParallel(PermutationState.PARTIAL);
  }

  @Order(3)
  @Test
  public void testPermutationAll() {
    testInParallel(PermutationState.ALL);
  }

  public void testInParallel(PermutationState permutationState) {
    AtomicInteger count = new AtomicInteger(0);
    List<Map<String, Object>> configurations = generateTestConfigurations(permutationState);
    for (Map<String, Object> config : configurations) {
      try {
        log.info("### Test for config: " + config);
        count.getAndIncrement();

        BdbAndFBase bdbAndFBase = getBdbAndFBaseUnique();

        performTest(bdbAndFBase, config);

        BerkleyDB berkleyDB = bdbAndFBase.getBerkleyDB();
        berkleyDB.closeDatabase();
        berkleyDB.removeDirectory();
      } catch (Exception e) {
        log.catching(e);
        throw new RuntimeException("Test failed for configuration: " + config, e);
      }
    }

    log.info("### Count of test configurations: " + count.get());
  }

  private void performTest(BdbAndFBase bdbAndFBase,
                           Map<String, Object> configuration)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {

    @SuppressWarnings("unchecked")
    Map<String, SType> csTypeMap = (Map<String, SType>) configuration.get("csTypeMap");
    IType indexType = (IType) configuration.get("indexType");
    Boolean compression = (Boolean) configuration.get("compression");
    LoadDataType loadDataType = (LoadDataType) configuration.get("loadDataType");

    FStore fStore = bdbAndFBase.getFStore();
    TType tableType = TType.TIME_SERIES;
    BType backendType = BType.BERKLEYDB;

    SProfile sProfile = loadData(loadDataType,
                                 fStore,
                                 tableName,
                                 csTypeMap,
                                 tableType,
                                 indexType,
                                 backendType,
                                 compression);

    TProfile tProfile;

    try {
      tProfile = this.fStore.loadJdbcTableMetadata(dbConnection, select, sProfile);
    } catch (TableNameEmptyException | SQLException e) {
      throw new RuntimeException(e);
    }

    // Stacked
    Map<BeginAndEnd<Integer, Integer>,
        BiConsumer<List<StackedColumn>, List<StackedColumn>>> stackedGivenThenMap = initializeStackedGivenThenMap();
    // Stacked filter
    Map<FSColAndRange, Consumer<List<StackedColumn>>> stackedFilterGivenThenMap = initializeStackedFilterGivenThenMap();

    // Populate When for Stacked
    for (BeginAndEnd<Integer, Integer> key : stackedGivenThenMap.keySet()) {
      whenListStackedColumnLastFirstName(bdbAndFBase.getFStore(), tProfile, key, stackedGivenThenMap);
    }
    // Populate When for Stacked filter
    for (FSColAndRange key : stackedFilterGivenThenMap.keySet()) {
      whenListStackedColumnFilter(bdbAndFBase.getFStore(), tProfile, key, stackedFilterGivenThenMap);
    }

    // Gantt
    Map<FSColAndRange, Consumer<List<GanttColumnCount>>> ganttGivenThenMap = initializeGanttGivenThenMap();

    // Populate When for Gantt
    for (FSColAndRange key : ganttGivenThenMap.keySet()) {
      whenListGanttColumn(bdbAndFBase.getFStore(), tProfile, key, ganttGivenThenMap);
    }

    // Raw
    Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<List<Object>>, List<List<Object>>>> rawGivenThenMap = initializeRawGivenThenMap();
    // Raw filter
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> rawFilterGivenThenMap = initializeRawFilterGivenThenMap();
    // Raw ResultSet
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> rawRSGivenThenMap = initializeRawRSGivenThenMap();
    // Raw ResultSet check empty
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> rawRSEmptyGivenThenMap = initializeRawRSEmptyGivenThenMap();

    // Populate When for Raw
    for (BeginAndEnd<Integer, Integer> key : rawGivenThenMap.keySet()) {
      whenRawExpectedActual(bdbAndFBase.getFStore(), tProfile, key, rawGivenThenMap);
    }

    // Populate When for Raw filter
    for (FSColAndRange key : rawFilterGivenThenMap.keySet()) {
      whenRawExpectedActualFilter(bdbAndFBase.getFStore(), tProfile, key, rawFilterGivenThenMap);
    }

    // Populate When for Raw ResultSet
    for (FSColAndRange key : rawRSGivenThenMap.keySet()) {
      whenRawExpectedActualRSFilter(bdbAndFBase.getFStore(), tProfile, key, Boolean.FALSE, rawRSGivenThenMap);
    }

    // Populate When for Raw ResultSet empty check
    for (FSColAndRange key : rawRSEmptyGivenThenMap.keySet()) {
      whenRawExpectedActualRSFilter(bdbAndFBase.getFStore(), tProfile, key, Boolean.TRUE, rawRSEmptyGivenThenMap);
    }
  }

  private Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<StackedColumn>, List<StackedColumn>>> initializeStackedGivenThenMap() {
    Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<StackedColumn>, List<StackedColumn>>> mapStacked = new HashMap<>();

    // Populate Given and Then for Stacked
    mapStacked.put(new BeginAndEnd<>(1, 2), this::thenListStackedColumn12);
    mapStacked.put(new BeginAndEnd<>(2, 3), this::thenListStackedColumn23);
    mapStacked.put(new BeginAndEnd<>(3, 4), this::thenListStackedColumn34);
    mapStacked.put(new BeginAndEnd<>(7, 9), this::thenListStackedColumn79);
    mapStacked.put(new BeginAndEnd<>(5, 9), this::thenListStackedColumn59);
    mapStacked.put(new BeginAndEnd<>(2, 9), this::thenListStackedColumn29);
    mapStacked.put(new BeginAndEnd<>(0, 10), this::thenListStackedColumn010);
    mapStacked.put(new BeginAndEnd<>(11, 11), this::thenListStackedColumn11);
    mapStacked.put(new BeginAndEnd<>(25, 27), this::thenListStackedColumn2527);

    return mapStacked;
  }

  private Map<FSColAndRange, Consumer<List<StackedColumn>>> initializeStackedFilterGivenThenMap() {
    Map<FSColAndRange, Consumer<List<StackedColumn>>> mapStackedFilter = new HashMap<>();

    // Populate Given and Then for Stacked filtered
    mapStackedFilter.put(new FSColAndRange(new FilterColumn("FIRSTNAME", "LASTNAME", "Petrov"), new BeginAndEnd<>(0, 10)), this::thenListStackedColumnFilterFirstNameLastName010);
    mapStackedFilter.put(new FSColAndRange(new FilterColumn("FIRSTNAME", "HOUSE", "2"), new BeginAndEnd<>(0, 10)), this::thenListStackedColumnFilterFirstNameHouse010);
    mapStackedFilter.put(new FSColAndRange(new FilterColumn("FIRSTNAME", "CITY", "Moscow"), new BeginAndEnd<>(0, 10)), this::thenListStackedColumnFilterFirstNameCity010);

    return mapStackedFilter;
  }

  private Map<FSColAndRange, Consumer<List<GanttColumnCount>>> initializeGanttGivenThenMap() {
    Map<FSColAndRange, Consumer<List<GanttColumnCount>>> mapGantt = new HashMap<>();

    // Populate Given and Then for Gantt
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(5, 7)), this::thenListGantt57);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(6, 7)), this::thenListGantt67);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(2, 6)), this::thenListGantt26);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(1, 6)), this::thenListGantt16);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(1, 7)), this::thenListGantt17);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(1, 11)), this::thenListGantt111);
    mapGantt.put(new FSColAndRange(new FSColumn("LASTNAME", "CITY"), new BeginAndEnd<>(1, 11)), this::thenListGanttLastNameCity111);
    mapGantt.put(new FSColAndRange(new FSColumn("LASTNAME", "HOUSE"), new BeginAndEnd<>(12, 14)), this::thenListGanttLastNameHouse1214);
    mapGantt.put(new FSColAndRange(new FSColumn("CITY", "HOUSE"), new BeginAndEnd<>(1, 11)), this::thenListGanttCityHouse111);
    mapGantt.put(new FSColAndRange(new FSColumn("CITY", "HOUSE"), new BeginAndEnd<>(1, 6)), this::thenListGanttCityHouse16);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(1, 6)), this::thenListGanttHouseCity16);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(12, 14)), this::thenListGanttHouseCity1214);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(11, 14)), this::thenListGanttHouseCity1114);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(1, 14)), this::thenListGanttHouseCity114);
    mapGantt.put(new FSColAndRange(new FSColumn("HOUSE", "CITY"), new BeginAndEnd<>(15, 25)), this::thenListGanttHouseCity1525);

    return mapGantt;
  }

  private Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<List<Object>>, List<List<Object>>>> initializeRawGivenThenMap() {
    Map<BeginAndEnd<Integer, Integer>, BiConsumer<List<List<Object>>, List<List<Object>>>> mapRaw = new HashMap<>();

    // Populate Given and Then for Raw
    mapRaw.put(new BeginAndEnd<>(0, 12), this::thenRaw012);
    mapRaw.put(new BeginAndEnd<>(7, 7), this::thenRaw77);
    mapRaw.put(new BeginAndEnd<>(5, 7), this::thenRaw57);
    mapRaw.put(new BeginAndEnd<>(6, 7), this::thenRaw67);
    mapRaw.put(new BeginAndEnd<>(1, 6), this::thenRaw16);
    mapRaw.put(new BeginAndEnd<>(8, 11), this::thenRaw811);
    mapRaw.put(new BeginAndEnd<>(1, 1), this::thenRawTimestamp11);

    return mapRaw;
  }

  private Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> initializeRawFilterGivenThenMap() {
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> mapRawFilter = new HashMap<>();

    // Populate Given and Then for Raw filter
    mapRawFilter.put(new FSColAndRange("ID", new BeginAndEnd<>(8, 11)), this::thenRawFilterId811);
    mapRawFilter.put(new FSColAndRange("FIRSTNAME", new BeginAndEnd<>(8, 11)), this::thenRawFilterFirstName811);
    mapRawFilter.put(new FSColAndRange("CITY", new BeginAndEnd<>(8, 11)), this::thenRawFilterCity811);

    return mapRawFilter;
  }

  private Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> initializeRawRSGivenThenMap() {
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> mapRawRS = new HashMap<>();

    // Populate Given and Then for Raw filter
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(7, 7)), this::thenRawRS77);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(5, 7)), this::thenRawRS57);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(6, 7)), this::thenRawRS67);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(1, 6)), this::thenRawRS16);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(8, 11)), this::thenRawRS811);

    return mapRawRS;
  }

  private Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> initializeRawRSEmptyGivenThenMap() {
    Map<FSColAndRange, BiConsumer<List<List<Object>>, List<List<Object>>>> mapRawRS = new HashMap<>();

    // Populate Given and Then for Raw check empty
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(1000, 2000)), this::thenRawRSEmpty);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(2000, 3000)), this::thenRawRSEmpty);
    mapRawRS.put(new FSColAndRange(1, new BeginAndEnd<>(Integer.MAX_VALUE - 1000, Integer.MAX_VALUE - 1)), this::thenRawRSEmpty);

    return mapRawRS;
  }

  private void thenListGantt57(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1),
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGantt67(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGantt26(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 3)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGantt16(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 4)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGantt17(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1),
        entry("Moscow", 4)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGantt111(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1),
        entry("Moscow", 8)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttLastNameCity111(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("Ivanov").gantt(Map.ofEntries(
        entry("Moscow", 4)
    )).build());
    expected.add(GanttColumnCount.builder().key("Petrov").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1),
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Sui").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Тихий").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Шаляпин").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Пирогов").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Semenov").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttLastNameHouse1214(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("Mirko").gantt(Map.ofEntries(
        entry("2", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Vedel").gantt(Map.ofEntries(
        entry("3", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("Tan").gantt(Map.ofEntries(
        entry("1", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttCityHouse111(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("Moscow").gantt(Map.ofEntries(
        entry("1", 8),
        entry("2", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("Yekaterinburg").gantt(Map.ofEntries(
        entry("1", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttCityHouse16(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("Moscow").gantt(Map.ofEntries(
        entry("1", 4),
        entry("2", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  public void thenListGanttHouseCity16(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 4)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttHouseCity1214(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("3").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttHouseCity1114(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("3").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttHouseCity114(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 9),
        entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Yekaterinburg", 1),
        entry("Moscow", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("3").gantt(Map.ofEntries(
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListGanttHouseCity1525(List<GanttColumnCount> actual) {
    List<GanttColumnCount> expected = new ArrayList<>();
    expected.add(GanttColumnCount.builder().key("1").gantt(Map.ofEntries(
        entry("Moscow", 1),
        entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("2").gantt(Map.ofEntries(
        entry("Moscow", 1),
        entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("3").gantt(Map.ofEntries(
        entry("Ufa", 1),
        entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumnCount.builder().key("4").gantt(Map.ofEntries(
        entry("Ufa", 1),
        entry("Moscow", 1)
    )).build());

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  private void thenListStackedColumn22(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName, "Ivanov"), 2);

    assertEquals(firstListStackedKey(firstName), "Alex");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(lastListStackedKey(lastName), "Ivanov");
    assertEquals(lastListStackedKey(firstName), "Ivan");
  }

  private void thenListStackedColumn23(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName, "Ivanov"), 1);

    assertEquals(firstListStackedKey(firstName), "Ivan");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(lastListStackedKey(lastName), "Petrov");
    assertEquals(lastListStackedKey(firstName), "Oleg");
  }

  private void thenListStackedColumn34(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Petrov");
    assertEquals(firstListStackedValue(lastName, "Petrov"), 1);

    assertEquals(firstListStackedKey(firstName), "Oleg");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(lastListStackedKey(lastName), "Sui");
    assertEquals(lastListStackedKey(firstName), "Lee");
  }

  private void thenListStackedColumn79(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Petrov");
    assertEquals(firstListStackedValue(lastName, "Petrov"), 1);

    assertEquals(firstListStackedKey(firstName), "Men");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(lastListStackedKey(lastName), "Шаляпин");
    assertEquals(lastListStackedKey(firstName), "Федор");
  }

  private void thenListStackedColumn59(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName, "Ivanov"), 2);

    assertEquals(firstListStackedKey(firstName), "Lee");
    assertEquals(firstListStackedValue(firstName), 2);

    assertEquals(lastListStackedKey(lastName), "Шаляпин");
    assertEquals(lastListStackedKey(firstName), "Федор");
  }

  private void thenListStackedColumn29(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName, "Ivanov"), 3);

    assertEquals(firstListStackedKey(firstName), "Ivan");
    assertEquals(firstListStackedValue(firstName), 1);

    assertEquals(lastListStackedKey(lastName), "Шаляпин");
    assertEquals(lastListStackedKey(firstName), "Федор");
  }

  private void thenListStackedColumn010(List<StackedColumn> lastName,
                                        List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName, "Ivanov"), 4);

    assertEquals(firstListStackedKey(firstName), "Alex");
    assertEquals(firstListStackedValue(firstName, "Alex"), 1);

    assertEquals(lastListStackedKey(lastName), "Пирогов");
    assertEquals(lastListStackedKey(firstName), "Петр");
  }

  private void thenListStackedColumn11(List<StackedColumn> lastName,
                                       List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Semenov");
    assertEquals(firstListStackedValue(lastName, "Semenov"), 1);

    assertEquals(firstListStackedKey(firstName), "Oleg");
    assertEquals(firstListStackedValue(firstName, "Oleg"), 1);
  }

  private void thenListStackedColumn2527(List<StackedColumn> lastName,
                                        List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Semenov");
    assertEquals(firstListStackedValue(lastName, "Semenov"), 1);

    assertEquals(firstListStackedKey(firstName), "Egor");
    assertEquals(firstListStackedValue(firstName, "Egor"), 1);

    assertEquals(lastListStackedKey(lastName), "Ivanov");
    assertEquals(lastListStackedKey(firstName), "Ivan");
  }

  private void thenListStackedColumn12(List<StackedColumn> lastName,
                                         List<StackedColumn> firstName) {
    assertEquals(firstListStackedKey(lastName), "Ivanov");
    assertEquals(firstListStackedValue(lastName), 2);
    assertEquals(firstListStackedKey(firstName), "Alex");
    assertEquals(firstListStackedValue(firstName), 1);
    assertEquals(lastListStackedKey(lastName), "Ivanov");
    assertEquals(lastListStackedKey(firstName), "Ivan");
  }

  private void thenListStackedColumnFilterFirstNameLastName010(List<StackedColumn> stackedColumnList) {
    assertEquals(firstListStackedKey(stackedColumnList), "Oleg");
    assertEquals(firstListStackedValue(stackedColumnList, "Oleg"), 1);

    assertEquals(lastListStackedKey(stackedColumnList), "Men");
  }

  private void thenListStackedColumnFilterFirstNameHouse010(List<StackedColumn> stackedColumnList) {
    assertEquals(firstListStackedKey(stackedColumnList), "Ivan");
    assertEquals(firstListStackedValue(stackedColumnList), 1);

    assertEquals(lastListStackedKey(stackedColumnList), "Lee");
  }

  private void thenListStackedColumnFilterFirstNameCity010(List<StackedColumn> stackedColumnList) {
    assertEquals(firstListStackedKey(stackedColumnList), "Alex");
    assertEquals(firstListStackedValue(stackedColumnList), 1);

    assertEquals(lastListStackedKey(stackedColumnList), "Петр");
  }

  private void thenRaw012(List<List<Object>> expected, List<List<Object>> actual) {
    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  private void thenRaw77(List<List<Object>> expected, List<List<Object>> actual) {
    assertEquals(expected.stream().filter(e -> e.get(0) == "7").count(), actual.size());
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()), actual);
  }

  private void thenRaw57(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "5" | e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  private void thenRaw67(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  private void thenRaw16(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3"
        | e.get(0) == "4" | e.get(0) == "5" | e.get(0) == "6");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  private void thenRaw811(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  private void thenRawTimestamp11(List<List<Object>> expected, List<List<Object>> actual) {
    List<List<Object>> expectedLocal = expected.stream().filter(f -> f.get(0).equals("1")).toList();

    assertEquals(expectedLocal.size(), actual.size());
    assertForRaw(expectedLocal, actual);
  }

  private void thenRawFilterId811(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(0))).collect(Collectors.toList()), actual);
  }

  private void thenRawFilterFirstName811(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(1))).collect(Collectors.toList()), actual);
  }

  private void thenRawFilterCity811(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(4))).collect(Collectors.toList()), actual);
  }

  private void thenRawRS77(List<List<Object>> expected, List<List<Object>> actual) {
    assertEquals(expected.stream().filter(e -> e.get(0) == "7").count(), actual.size());
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()), actual);
  }

  private void thenRawRS57(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "5" | e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  actual);
  }

  private void thenRawRS67(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  actual);
  }

  private void thenRawRS16(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3"
        | e.get(0) == "4" | e.get(0) == "5" | e.get(0) == "6");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  actual);
  }

  private void thenRawRS811(List<List<Object>> expected, List<List<Object>> actual) {
    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  actual);
  }

  private void thenRawRSEmpty(List<List<Object>> expected, List<List<Object>> actual) {
  }
}

