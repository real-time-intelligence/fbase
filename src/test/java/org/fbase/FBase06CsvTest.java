package org.fbase;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.sql.BatchResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public class FBase06CsvTest {
  @TempDir
  static File databaseDir;

  private BerkleyDB berkleyDB;
  private FStore fStore;
  private String tableName = "csv_table_test";

  @BeforeEach
  public void init() throws IOException {
    String dbDir = databaseDir.getAbsolutePath() + FILE_SEPARATOR + "csv";
    this.berkleyDB = new BerkleyDB(dbDir, true);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbDir).setBlockSize(16);

    try {
      FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
      fStore = fBase.getFStore();
    } catch (Exception e) {
      log.catching(e);
    }
  }

  @Test
  public void putDataBatchCompressOneFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressOneFetchTwoTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 2, true);
  }

  @Test
  public void putDataBatchCompressOneFetchThreeTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 3, false);
  }

  @Test
  public void putDataBatchCompressOneFetchFourTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 4, true);
  }

  @Test
  public void putDataBatchCompressOneFetchFiveTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 5, false);
  }

  @Test
  public void putDataBatchCompressOneFetchSixTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 1);
    assertDataCsvBatchTest(true, 6, false);
  }

  @Test
  public void putDataBatchCompressTwoFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 2);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressThreeFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 3);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressFourFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 4);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressFiveFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 5);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressSixFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 6);
    assertDataCsvBatchTest(true, 1, true);
  }

  @Test
  public void putDataBatchCompressTwoFetchTwoTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 2);
    assertDataCsvBatchTest(true, 2, true);
  }

  @Test
  public void putDataBatchCompressTwoFetchThreeTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(true, 2);
    assertDataCsvBatchTest(true, 3, false);
  }

  @Test
  public void putDataBatchOneFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchOneFetchTwoTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 2, true);
  }

  @Test
  public void putDataBatchOneFetchThreeTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 3, false);
  }

  @Test
  public void putDataBatchOneFetchFourTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 4, true);
  }

  @Test
  public void putDataBatchOneFetchFiveTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 5, false);
  }

  @Test
  public void putDataBatchOneFetchSixTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 1);
    assertDataCsvBatchTest(false, 6, false);
  }

  @Test
  public void putDataBatchTwoFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 2);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchThreeFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 3);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchFourFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 4);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchFiveFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 5);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchSixFetchOneTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 6);
    assertDataCsvBatchTest(false, 1, true);
  }

  @Test
  public void putDataBatchTwoFetchTwoTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 2);
    assertDataCsvBatchTest(false, 2, true);
  }

  @Test
  public void putDataBatchTwoFetchThreeTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSet(false, 2);
    assertDataCsvBatchTest(false, 3, false);
  }

  @Test
  public void putDataBatchTwoFetchDynamicTest() throws SqlColMetadataException, IOException {
    putDataCsvBatchResultSetLarge(false, 2);
    assertDataCsvBatchTestLarge(false, 10, false);
  }

  private void putDataCsvBatchResultSetLarge(boolean compression, int fBaseBatchSize) throws SqlColMetadataException {
    putDataCsvBatchResultSet("file-l.csv", compression, fBaseBatchSize);
  }

  private void putDataCsvBatchResultSet(boolean compression, int fBaseBatchSize) throws SqlColMetadataException {
    putDataCsvBatchResultSet("file.csv", compression, fBaseBatchSize);
  }

  private void putDataCsvBatchResultSet(String fileNameCsv, boolean compression, int fBaseBatchSize) throws SqlColMetadataException {
    String csvSplitBy = ",";

    String fileName = new File("").getAbsolutePath()  + FILE_SEPARATOR +
        Paths.get("src","test", "resources", "csv", fileNameCsv);

    TProfile tProfile = getTProfile(fileName, csvSplitBy, compression);

    String tableName = tProfile.getTableName();

    fStore.putDataCsvBatch(tableName, fileName, csvSplitBy, fBaseBatchSize);
  }

  private void assertDataCsvBatchTestLarge(boolean compression, int fetchSize, boolean eventFetchSize) throws IOException {
    assertDataCsvBatchTest("file-l.csv", compression, fetchSize, eventFetchSize);
  }

  private void assertDataCsvBatchTest(boolean compression, int fetchSize, boolean eventFetchSize) throws IOException {
    assertDataCsvBatchTest("file.csv", compression, fetchSize, eventFetchSize);
  }

  private void assertDataCsvBatchTest(String fileNameCsv, boolean compression, int fetchSize, boolean eventFetchSize) throws IOException {
    String csvSplitBy = ",";

    String fileName = new File("").getAbsolutePath()  + FILE_SEPARATOR +
        Paths.get("src","test", "resources", "csv", fileNameCsv);

    TProfile tProfile = getTProfile(fileName, csvSplitBy, compression);

    String tableName = tProfile.getTableName();

    String expected = readFile(fileName, Charset.defaultCharset());

    List<List<Object>> rawDataAll = new ArrayList<>();

    BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, fetchSize);

    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      if (eventFetchSize) {
        assertEquals(fetchSize, var.size());
      }
      rawDataAll.addAll(var);
    }

    String actual = toCsvFile(rawDataAll, tProfile, csvSplitBy);

    assertEquals(expected, actual);
  }

  private TProfile getTProfile(String fileName, String csvSplitBy, boolean compression) {
    TProfile tProfile;
    try {
      SProfile sProfile = SProfile.builder()
          .tableName(tableName)
          .tableType(TType.REGULAR)
          .indexType(IType.GLOBAL)
          .backendType(BType.BERKLEYDB)
          .compression(compression)
          .csTypeMap(new HashMap<>()).build();

      MetadataHandler.loadMetadataFromCsv(fileName, csvSplitBy, sProfile);

      tProfile = fStore.loadCsvTableMetadata(fileName, csvSplitBy, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    return tProfile;
  }

  static String readFile(String path, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding).replace("\r", "");
  }

  private String toCsvFile(List<List<Object>> data, TProfile tProfile, String csvSplitBy) {
    StringBuilder output = new StringBuilder();

    // headers
    AtomicInteger headerCounter = new AtomicInteger(0);
    List<CProfile> cProfiles = tProfile.getCProfiles();
    cProfiles.stream()
        .sorted(Comparator.comparing(CProfile::getColId))
        .forEach(cProfile -> {
          headerCounter.getAndAdd(1);
          output.append(cProfile.getColName()).append(headerCounter.get() < cProfiles.size() ? csvSplitBy : "");
        });

    output.append("\n");

    // data
    AtomicInteger counter = new AtomicInteger(0);
    for (List<Object> rowData : data) {
      counter.getAndAdd(1);
      for (int i = 0; i < rowData.size(); i++) {
        output.append(rowData.get(i).toString());
        if (i < rowData.size() - 1) {
          output.append(csvSplitBy);
        }
      }

      if (counter.get() < data.size()) {
        output.append("\n");
      }
    }

    return output.toString();
  }

  @AfterEach
  public void closeDb() {
    berkleyDB.closeDatabase();
    databaseDir.deleteOnExit();
  }
}
