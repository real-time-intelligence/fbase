package org.fbase.common;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
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
  protected List<List<Object>> data02;
  protected List<List<Object>> data03;

  private TProfile tProfile;
  protected List<CProfile> cProfiles;

  protected String tableName = "direct_table_test";

  protected String testMessage1 = "Test message 1";
  protected String testMessage2 = "Test message 2";
  protected String testMessage3 = "Test message 3";
  protected Map<String, Integer> testMap1;
  protected Map<String, Integer> testMap2;
  protected Map<String, Integer> testMap3;
  protected String[] array1;
  protected int kMap;

  @BeforeAll
  public void initBackendAndLoad() throws IOException {
    berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, true);

    fBaseConfig = new FBaseConfig().setConfigDirectory(BERKLEY_DB_DIR).setBlockSize(16);
    fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    kMap = 2;
    testMap1 = new HashMap<>();
    testMap1.put("val1", 1);
    testMap1.put("val2", 2);
    testMap1.put("val3", 3);

    testMap2 = new HashMap<>();
    testMap2.put("val4", 4);
    testMap2.put("val5", 5);
    testMap2.put("val6", 6);

    testMap3 = new HashMap<>();

    array1 = new String[2];
    array1[0] = "array value 1";
    array1[1] = "array value 2";
  }

  protected void putDataDirect(SProfile sProfile) {
    fStore = fBase.getFStore();

    try {
      tProfile = loadTableMetadata(sProfile);

      String tableName = tProfile.getTableName();
      cProfiles = tProfile.getCProfiles();

      data01 = loadData(cProfiles, 0, kMap, testMap1, testMessage1, array1);
      data02 = loadData(cProfiles, 10, kMap + 10, testMap2, testMessage2, array1);
      data03 = loadData(cProfiles, 20, kMap + 20, testMap3, testMessage3, array1);

      fStore.putDataDirect(tableName, data01);
      fStore.putDataDirect(tableName, data02);
      fStore.putDataDirect(tableName, data03);

    } catch (SqlColMetadataException | EnumByteExceedException | TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
  }

  private TProfile loadTableMetadata(SProfile sProfile) throws TableNameEmptyException {
    return fStore.loadDirectTableMetadata(sProfile);
  }

  private List<List<Object>> loadData(List<CProfile> cProfiles, int start, int end, Map<String, Integer> mapData,
      String messageData, String[] array) {
    List<List<Object>> data = new ArrayList<>();
    initializeDataStructure(cProfiles, data);

    for (int i = start; i < end; i++) {
      final int index = i;
      cProfiles.forEach(v -> addToDataStructure(v, data, index, mapData, messageData, array));
    }
    return data;
  }

  private void initializeDataStructure(List<CProfile> cProfiles, List<List<Object>> data) {
    cProfiles.forEach(v -> data.add(v.getColId(), new ArrayList<>()));
  }

  private void addToDataStructure(CProfile v, List<List<Object>> data, int index, Map<String, Integer> mapData,
      String messageData, String[] array) {
    Object valueToAdd = determineValue(v, index, mapData, messageData, array);
    data.get(v.getColId()).add(valueToAdd);
  }

  private Object determineValue(CProfile profile, int index, Map<String, Integer> mapData, String messageData, String[] array) {
    DataType dType = profile.getCsType().getDType();
    if (DataType.MAP.equals(dType)) {
      return mapData;
    } else if (DataType.LONG.equals(dType)) {
      return index;
    } else if (DataType.VARCHAR.equals(dType)) {
      return messageData;
    } else if (DataType.ARRAY.equals(dType)) {
      return array;
    }
    return null;
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

  protected void compareKeySetForMapDataType(Map<String, Integer> expectedMap, List<StackedColumn> listMapActual) {
    assertEquals(expectedMap.keySet(),
        listMapActual.stream().filter(f -> f.getKeyCount()
            .keySet().equals(expectedMap.keySet()))
            .findAny()
            .orElseThrow()
            .getKeyCount()
            .keySet());
  }

  public List<StackedColumn> getListStackedDataBySqlCol(FStore fStore, TProfile tProfile,
      List<CProfile> cProfiles, String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfiles.stream()
        .filter(k -> k.getColName().equalsIgnoreCase(colName)).findAny().orElseThrow(), begin, end);
  }

  public Object findListStackedKey(List<StackedColumn> list, String filter) {
    for (StackedColumn stackedColumn : list) {
      if (stackedColumn.getKeyCount().containsKey(filter)) {
        return stackedColumn.getKeyCount().entrySet()
            .stream()
            .filter((k) -> k.getKey().equals(filter)).findAny().orElseThrow().getKey();
      }
    }
    return null;
  }

  public Object findListStackedValue(List<StackedColumn> list, String filter) {
    for (StackedColumn stackedColumn : list) {
      if (stackedColumn.getKeyCount().containsKey(filter)) {
        return stackedColumn.getKeyCount().entrySet()
            .stream()
            .filter((k) -> k.getKey().equals(filter)).findAny().orElseThrow().getValue();
      }
    }
    return null;
  }

  public List<StackedColumn> getDataStackedColumn(String colName, int begin, int end)
      throws BeginEndWrongOrderException, SqlColMetadataException {
    return getListStackedDataBySqlCol(fStore, tProfile, cProfiles, colName, begin, end);
  }

  public List<List<Object>> getRawDataAll(int begin, int end) {
    return fStore.getRawDataAll(tProfile.getTableName(), begin, end);
  }

  @AfterAll
  public void closeDb() throws SQLException, IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
