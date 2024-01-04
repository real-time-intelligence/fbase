package org.fbase.common;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.metadata.DataType;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractDirectTest {
  protected final String BERKLEY_DB_DIR = Paths.get(".").toAbsolutePath().normalize() + FILE_SEPARATOR + "database";
  protected BerkleyDB berkleyDB;

  protected FBaseConfig fBaseConfig;
  protected FBase fBase;
  protected FStore fStore;

  protected List<List<Object>> data01;

  private TProfile tProfile;
  protected List<CProfile> cProfiles;

  protected String tableName = "direct_table_test";

  protected String testMessage = "Test message";
  protected Map<String, Integer> integerMap;
  protected int kMap;

  @BeforeAll
  public void initBackendAndLoad() throws IOException {
    berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, true);

    fBaseConfig = new FBaseConfig().setConfigDirectory(BERKLEY_DB_DIR).setBlockSize(16);
    fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    kMap = 2;
    integerMap = new HashMap<>();
    integerMap.put("val1", 1);
    integerMap.put("val2", 2);
    integerMap.put("val3", 3);
  }

  protected void putDataDirect(SProfile sProfile) {
    fStore = fBase.getFStore();

    try {
      try {
        tProfile = fStore.loadDirectTableMetadata(sProfile);
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      String tableName = tProfile.getTableName();
      cProfiles = tProfile.getCProfiles();

      // load data here
      data01 = new ArrayList<>();
      cProfiles.forEach(v -> data01.add(v.getColId(), new ArrayList<>()));

      for (int i = 0; i < kMap; i++) {
        int finalI = i;
        cProfiles.forEach(v -> {
          if (DataType.MAP.equals(v.getCsType().getDType())) {
            data01.get(v.getColId()).add(integerMap);
          } else if (DataType.LONG.equals(v.getCsType().getDType())) {
            data01.get(v.getColId()).add(finalI);
          } else if (DataType.VARCHAR.equals(v.getCsType().getDType())) {
            data01.get(v.getColId()).add(testMessage);
          }
        });
      }

      fStore.putDataDirect(tableName, data01);
    } catch (SqlColMetadataException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void loadExpected(List<List<Object>> expected) {
    expected.add(Arrays.asList(new String[]{"1", "Alex", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"2", "Ivan", "Ivanov", "2", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"3", "Oleg", "Petrov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"4", "Lee", "Sui", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"5", "Lee", "Ivanov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"6", "Lee", "Ivanov", "2", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"7", "Men", "Petrov", "1", "Yekaterinburg", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"8", "Ion", "Тихий", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"9", "Федор", "Шаляпин", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"10", "Петр", "Пирогов", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"11", "Oleg", "Semenov", "1", "Moscow", "01.01.2023 01:01:01"}));
    expected.add(Arrays.asList(new String[]{"12", "Oleg", "Mirko", "2", "Yekaterinburg", "01.01.2023 01:01:01"}));
  }

  protected List<GanttColumn> getDataGanttColumn(String firstColName, String secondColName, int begin, int end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    CProfile firstLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(firstColName))
        .findAny().get();
    CProfile secondLevelGroupBy = cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(secondColName))
        .findAny().get();

    return getListGanttColumnTwoLevelGrouping(fStore, tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  public List<GanttColumn> getListGanttColumnTwoLevelGrouping(FStore fStore, TProfile tProfile,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    return fStore.getGColumnListTwoLevelGroupBy(tProfile.getTableName(), firstLevelGroupBy, secondLevelGroupBy, begin, end);
  }

  protected void assertForGanttColumn(List<GanttColumn> expected, List<GanttColumn> actual) {
    expected.forEach(e ->
        assertEquals(e.getGantt(), actual.stream()
            .filter(f -> f.getKey().equals(e.getKey()))
            .findAny()
            .orElseThrow()
            .getGantt()));
  }

  public List<StackedColumn> getListStackedDataBySqlCol(FStore fStore, TProfile tProfile,
      List<CProfile> cProfiles, String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow(), begin, end);
  }

  public Object lastListStackedKey(List<StackedColumn> list) {
    return list.stream().reduce((first, second) -> second).orElseThrow()
        .getKeyCount().entrySet().stream().reduce((first, second) -> second).orElseThrow().getKey();
  }

  public Object firstListStackedKey(List<StackedColumn> list) {
    return list.stream().findFirst().orElseThrow()
        .getKeyCount().entrySet().stream().findFirst().orElseThrow().getKey();
  }

  public Object firstListStackedValue(List<StackedColumn> list) {
    return list.stream().findFirst().orElseThrow()
        .getKeyCount().entrySet().stream().findFirst().orElseThrow().getValue();
  }

  public List<StackedColumn> getDataStackedColumn(String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(fStore, tProfile, cProfiles, colName, begin, end);
  }

  public List<List<Object>> getRawDataAll(int begin, int end) {
    return fStore.getRawDataAll(tProfile.getTableName(), begin, end);
  }

  public List<List<Object>> getRawDataByColumn(CProfile cProfile, int begin, int end) {
    return fStore.getRawDataByColumn(tProfile.getTableName(), cProfile, begin, end);
  }

  public CProfile getCProfileByColumnName(String colName) {
    return cProfiles.stream().filter(f -> f.getColName().equals(colName)).findAny().orElseThrow();
  }

  public List<StackedColumn> getStackedData(String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(fStore, tProfile, cProfiles, colName, begin, end);
  }

  protected void assertForRaw(List<List<Object>> expected, List<List<Object>> actual) {
    for (int i = 0; i < expected.size(); i++) {
      for (int j = 0; j < expected.get(i).size(); j++) {
        assertEquals(String.valueOf(expected.get(i).get(j)), String.valueOf(actual.get(i).get(j)));
      }
    }
  }

  @AfterAll
  public void closeDb() throws SQLException, IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
