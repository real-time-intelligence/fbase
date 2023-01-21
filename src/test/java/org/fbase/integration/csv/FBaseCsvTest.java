package org.fbase.integration.csv;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * <a href="https://github.com/h2oai/db-benchmark">https://github.com/h2oai/db-benchmark</a>
 */
@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseCsvTest {
  private FStore fStore;
  private BerkleyDB berkleyDB;
  private String fileName;
  private String targetFBase;

  private String tableName = "csv_table_test";

  @BeforeAll
  public void initialLoading() throws IOException {
    fileName = getTestDbFolder("C:\\Users\\.benchmark\\git\\db-benchmark", "G1_1e7_1e2_0_0_example.csv");

    targetFBase = getTestDbFolder("C:\\Users\\.benchmark", "fbase-data");

    this.berkleyDB = new BerkleyDB(targetFBase, true);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(targetFBase).setBlockSize(16);

    try {
      FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
      fStore = fBase.getFStore();
    } catch (Exception e) {
      log.catching(e);
    }
  }

  @Test
  public void loadData() throws SqlColMetadataException {
    String csvSplitBy = ",";

    TProfile tProfile;
    try {
      tProfile = fStore.loadCsvTableMetadata(fileName, csvSplitBy, getSProfile());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    fStore.putDataCsvBatch(tProfile.getTableName(), fileName, csvSplitBy, 1);

    log.info(fStore.getRawDataAll(tProfile.getTableName()));
  }

  private SProfile getSProfile() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setIsTimestamp(false);

    Map<String, CSType> csTypeMap = new HashMap<>();
    sProfile.setCsTypeMap(csTypeMap);

    return sProfile;
  }

  String getTestDbFolder(String rootFolder, String folderName) {
    return String.format("%s%s" + folderName, Paths.get(rootFolder).toAbsolutePath().normalize(), FILE_SEPARATOR);
  }

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
