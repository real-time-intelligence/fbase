package org.fbase;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

  @BeforeAll
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
  public void putDataCsvBatchTest() throws SqlColMetadataException, IOException {
    String csvSplitBy = ",";

    String fileName = new File("").getAbsolutePath()  + FILE_SEPARATOR +
        Paths.get("src","test", "resources", "csv", "file.csv");

    TProfile tProfile;
    try {
      tProfile = fStore.loadCsvTableMetadata(fileName, csvSplitBy,
          SProfile.builder().tableName(tableName).isTimestamp(false).csTypeMap(new HashMap<>()).build());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    String tableName = tProfile.getTableName();

    fStore.putDataCsvBatch(tableName, fileName, csvSplitBy, 1);

    String expected = readFile(fileName, Charset.defaultCharset());

    List<List<Object>> rawDataAll = fStore.getRawDataAll(tableName);
    String actual = toCsvFile(rawDataAll, tProfile, csvSplitBy);

    log.info(fStore.getRawDataAll(tableName));

    assertEquals(expected, actual);
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

  @AfterAll
  public void closeDb() {
    berkleyDB.closeDatabase();
  }
}
