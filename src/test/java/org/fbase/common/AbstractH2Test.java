package org.fbase.common;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
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
import org.fbase.model.Person;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.source.H2Database;
import org.fbase.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractH2Test implements JdbcSource {
  protected final String BERKLEY_DB_DIR = Paths.get(".").toAbsolutePath().normalize() + FILE_SEPARATOR + "database";
  protected BerkleyDB berkleyDB;

  protected H2Database h2Db;

  protected Connection dbConnection;

  protected FBaseConfig fBaseConfig;
  protected FBase fBase;
  protected FStore fStore;

  protected List<List<Object>> data01;
  protected List<List<Object>> data02;
  protected List<List<Object>> data03;
  protected List<List<Object>> data04;
  protected List<List<Object>> data05;
  protected List<List<Object>> data06;

  private TProfile tProfile;
  private List<CProfile> cProfiles;
  private String select = "SELECT * FROM person WHERE ROWNUM < 2";

  @BeforeAll
  public void initBackendAndLoad() throws SQLException, IOException {
    h2Db = new H2Database("jdbc:h2:mem:test");
    berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, true);

    fBaseConfig = new FBaseConfig().setConfigDirectory(BERKLEY_DB_DIR).setBlockSize(16);
    fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    dbConnection = h2Db.getConnection();

    h2Db.execute("CREATE TABLE PERSON (id INT PRIMARY KEY, "
        + "firstname VARCHAR(64), lastname VARCHAR(64), house INT, city VARCHAR(64))");

    h2Db.insert(Person.builder().id(1).firstname("Alex").lastname("Ivanov").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(2).firstname("Ivan").lastname("Ivanov").house(2).city("Moscow").build());
    h2Db.insert(Person.builder().id(3).firstname("Oleg").lastname("Petrov").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(4).firstname("Lee").lastname("Sui").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(5).firstname("Lee").lastname("Ivanov").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(6).firstname("Lee").lastname("Ivanov").house(2).city("Moscow").build());

    h2Db.loadSqlColMetadataList("SELECT * FROM person");
    data01 = h2Db.getData("SELECT * FROM person");

    h2Db.insert(Person.builder().id(7).firstname("Men").lastname("Petrov").house(1).city("Yekaterinburg").build());
    data02 = h2Db.getData("SELECT * FROM person WHERE id=7");

    h2Db.insert(Person.builder().id(8).firstname("Ion").lastname("Тихий").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(9).firstname("Федор").lastname("Шаляпин").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(10).firstname("Петр").lastname("Пирогов").house(1).city("Moscow").build());
    data03 = h2Db.getData("SELECT * FROM person WHERE id=10 OR id=8 OR id=9");

    h2Db.insert(Person.builder().id(11).firstname("Oleg").lastname("Semenov").house(1).city("Moscow").build());
    data04 = h2Db.getData("SELECT * FROM person WHERE id=11");

    h2Db.insert(Person.builder().id(12).firstname("Oleg").lastname("Mirko").house(2).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(13).firstname("Oleg").lastname("Vedel").house(3).city("Moscow").build());
    h2Db.insert(Person.builder().id(14).firstname("Oleg").lastname("Tan").house(1).city("Moscow").build());
    data05 = h2Db.getData("SELECT * FROM person WHERE id=12 OR id=13 OR id=14");

    h2Db.insert(Person.builder().id(15).firstname("Egor").lastname("Semenov").house(1).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(16).firstname("Egor").lastname("Semenov").house(1).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(17).firstname("Egor").lastname("Semenov").house(1).city("Moscow").build());
    h2Db.insert(Person.builder().id(18).firstname("Egor").lastname("Semenov").house(2).city("Moscow").build());
    h2Db.insert(Person.builder().id(19).firstname("Egor").lastname("Semenov").house(2).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(20).firstname("Egor").lastname("Semenov").house(2).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(21).firstname("Egor").lastname("Semenov").house(3).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(22).firstname("Egor").lastname("Semenov").house(3).city("Yekaterinburg").build());
    h2Db.insert(Person.builder().id(23).firstname("Egor").lastname("Semenov").house(3).city("Ufa").build());
    h2Db.insert(Person.builder().id(24).firstname("Egor").lastname("Semenov").house(4).city("Ufa").build());
    h2Db.insert(Person.builder().id(25).firstname("Egor").lastname("Semenov").house(4).city("Moscow").build());
    data06 = h2Db.getData("SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
        + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25");
  }

  protected void putData(Map<String, SType> csTypeMap) {
    fStore = fBase.getFStore();

    cProfiles = h2Db.getCProfileList().stream()
            .map(col -> col.toBuilder()
                    .colId(col.getColId())
                    .colName(col.getColName())
                    .colDbTypeName(col.getColDbTypeName())
                    .colSizeDisplay(col.getColSizeDisplay())
                    .colSizeSqlType(col.getColSizeSqlType())
                    .csType(CSType.builder()
                            .isTimeStamp(col.getColName().equalsIgnoreCase("ID"))
                            .sType(csTypeMap.get(col.getColName()))
                            .build())
                    .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setCsTypeMap(new HashMap<>());

      csTypeMap.forEach((k,v) -> {
        if (k.equals("ID")) {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().isTimeStamp(true).sType(v).build());
        } else {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().sType(v).build());
        }
      });

      tProfile = fStore.getTableMetadata(dbConnection, select, sProfile);
      fStore.putDataDirect(tProfile, data01);
      fStore.putDataDirect(tProfile, data02);
      fStore.putDataDirect(tProfile, data03);
      fStore.putDataDirect(tProfile, data04);
      fStore.putDataDirect(tProfile, data05);
      fStore.putDataDirect(tProfile, data06);
    } catch (SqlColMetadataException | SQLException | EnumByteExceedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void putDataDirect(Map<String, SType> csTypeMap) {
    fStore = fBase.getFStore();

    cProfiles = h2Db.getCProfileList().stream()
        .map(col -> col.toBuilder()
            .colId(col.getColId())
            .colName(col.getColName())
            .colDbTypeName(col.getColDbTypeName())
            .colSizeDisplay(col.getColSizeDisplay())
            .colSizeSqlType(col.getColSizeSqlType())
            .csType(CSType.builder()
                .isTimeStamp(col.getColName().equalsIgnoreCase("ID"))
                .sType(csTypeMap.get(col.getColName()))
                .build())
            .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setCsTypeMap(new HashMap<>());

      csTypeMap.forEach((k,v) -> {
        if (k.equals("ID")) {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().isTimeStamp(true).sType(v).build());
        } else {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().sType(v).build());
        }
      });

      tProfile = fStore.getTableMetadata(dbConnection, select, sProfile);

      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6");
      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=7");
      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=10 OR id=8 OR id=9");
      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=11");
      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=12 OR id=13 OR id=14");
      h2Db.putDataDirect(fStore, tProfile,
          "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
              + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25");

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void putDataBatch(Map<String, SType> csTypeMap) {
    fStore = fBase.getFStore();

    cProfiles = h2Db.getCProfileList().stream()
        .map(col -> col.toBuilder()
            .colId(col.getColId())
            .colName(col.getColName())
            .colDbTypeName(col.getColDbTypeName())
            .colSizeDisplay(col.getColSizeDisplay())
            .colSizeSqlType(col.getColSizeSqlType())
            .csType(CSType.builder()
                .isTimeStamp(col.getColName().equalsIgnoreCase("ID"))
                .sType(csTypeMap.get(col.getColName()))
                .build())
            .build()).toList();

    try {
      SProfile sProfile = new SProfile();
      sProfile.setCsTypeMap(new HashMap<>());

      csTypeMap.forEach((k,v) -> {
        if (k.equals("ID")) {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().isTimeStamp(true).sType(v).build());
        } else {
          sProfile.getCsTypeMap().put(k, new CSType().toBuilder().sType(v).build());
        }
      });

      tProfile = fStore.getTableMetadata(dbConnection, select, sProfile);

      Integer fBaseBatchSize = 3;
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=1 OR id=2 OR id=3 OR id=4 OR id=5 OR id=6", fBaseBatchSize);
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=7", fBaseBatchSize);
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=10 OR id=8 OR id=9", fBaseBatchSize);
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=11", fBaseBatchSize);
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=12 OR id=13 OR id=14", fBaseBatchSize);
      h2Db.putDataBatch(fStore, tProfile,
          "SELECT * FROM person WHERE id=15 OR id=16 OR id=17 OR id=18 OR id=19 OR id=20"
              + " OR id=21 OR id=22 OR id=23 OR id=24 OR id=25", fBaseBatchSize);

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void loadExpected(List<List<Object>> expected) {
    expected.add(Arrays.asList(new String[]{"1", "Alex", "Ivanov", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"2", "Ivan", "Ivanov", "2", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"3", "Oleg", "Petrov", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"4", "Lee", "Sui", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"5", "Lee", "Ivanov", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"6", "Lee", "Ivanov", "2", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"7", "Men", "Petrov", "1", "Yekaterinburg"}));
    expected.add(Arrays.asList(new String[]{"8", "Ion", "Тихий", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"9", "Федор", "Шаляпин", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"10", "Петр", "Пирогов", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"11", "Oleg", "Semenov", "1", "Moscow"}));
    expected.add(Arrays.asList(new String[]{"12", "Oleg", "Mirko", "2", "Yekaterinburg"}));
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
    return fStore.getGColumnListTwoLevelGroupBy(tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
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
    return fStore.getSColumnListByCProfile(tProfile, cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow(),begin, end);
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
    return fStore.getRawDataAll(tProfile, begin, end);
  }

  public List<List<Object>> getRawDataByColumn(CProfile cProfile, int begin, int end) {
    return fStore.getRawDataByColumn(tProfile, cProfile,  begin, end);
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

    h2Db.execute("DROP ALL OBJECTS");
  }
}
