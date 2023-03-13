package org.fbase.integration.csv;

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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.TType;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.sql.BatchResultSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

/**
 * <a href="https://github.com/h2oai/db-benchmark">https://github.com/h2oai/db-benchmark</a>
 */
@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseCsvRecursiveTest {
  private FStore fStore;
  private BerkleyDB berkleyDB;

  private String tableName = "csv_table_test";

  @TempDir
  static File databaseDir;

  private void loadData(String fileNameCsv, int fBaseBatchSize) throws SqlColMetadataException, IOException {
    // Initialize
    String dbDir = databaseDir.getAbsolutePath() + FILE_SEPARATOR + "csv";
    this.berkleyDB = new BerkleyDB(dbDir, true);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbDir).setBlockSize(16);

    try {
      FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
      fStore = fBase.getFStore();
    } catch (Exception e) {
      log.catching(e);
    }

    // Load data
    String fileName = new File("").getAbsolutePath()  + FILE_SEPARATOR +
        Paths.get("src","test", "resources", "csv", fileNameCsv);

    String csvSplitBy = ",";

    TProfile tProfile;
    try {
      tProfile = fStore.loadCsvTableMetadata(fileName, csvSplitBy, getSProfile());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    fStore.putDataCsvBatch(tProfile.getTableName(), fileName, csvSplitBy, fBaseBatchSize);
  }

  @Test
  public void fBaseBatchSizeFetchSizeCompressionRecursiveTest() throws IOException, SqlColMetadataException {
    fBaseBatchSizeFetchSizeRecursive(true);
  }

  @Test
  public void fBaseBatchSizeFetchSizeNonCompressionRecursiveTest() throws IOException, SqlColMetadataException {
    fBaseBatchSizeFetchSizeRecursive(false);
  }

  private void fBaseBatchSizeFetchSizeRecursive(boolean compression) throws IOException, SqlColMetadataException {
    String fileNameCsv = "file-l.csv";
    String csvSplitBy = ",";

    String fileName = new File("").getAbsolutePath()  + FILE_SEPARATOR +
        Paths.get("src","test", "resources", "csv", fileNameCsv);

    String expected = readFile(fileName, Charset.defaultCharset());

    for (int fBaseBatchSize = 0; fBaseBatchSize < expected.lines().count() + 3; fBaseBatchSize++) {
      loadData(fileNameCsv, fBaseBatchSize);

      for (int fetchSize = 0; fetchSize < expected.lines().count() + 3; fetchSize++) {

        TProfile tProfile = getTProfile(fileName, csvSplitBy, compression);
        String tableName = tProfile.getTableName();
        List<List<Object>> rawDataAll = new ArrayList<>();

        BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, fetchSize);

        while (batchResultSet.next()) {
          rawDataAll.addAll(batchResultSet.getObject());
        }

        String actual = toCsvFile(rawDataAll, tProfile, csvSplitBy);

        assertEquals(expected, actual);
      }

      log.info("FBaseBatchSize: " + fBaseBatchSize);

      berkleyDB.closeDatabase();
      berkleyDB.removeDirectory();
    }
  }

  private TProfile getTProfile(String fileName, String csvSplitBy, boolean compression) {
    TProfile tProfile;
    try {
      tProfile = fStore.loadCsvTableMetadata(fileName, csvSplitBy,
          SProfile.builder()
              .tableName(tableName)
              .tableType(TType.REGULAR)
              .compression(compression)
              .csTypeMap(new HashMap<>()).build());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    return tProfile;
  }

  private SProfile getSProfile() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setCompression(false);
    sProfile.setTableType(TType.REGULAR);

    Map<String, CSType> csTypeMap = new HashMap<>();
    sProfile.setCsTypeMap(csTypeMap);

    return sProfile;
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

}
