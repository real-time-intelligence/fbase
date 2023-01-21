package org.fbase.integration.mocked;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.source.ClickHouse;
import org.fbase.source.ClickHouseMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public class FBase05ClickHouseMockTest implements ClickHouse {
  private FStore fStore;
  private TProfile tProfile;
  private List<CProfile> cProfiles;
  @TempDir
  static File databaseDir;

  public final String FILE_SEPARATOR = System.getProperty("file.separator");

  private ClickHouseMock clickHouseMock;
  private BerkleyDB berkleyDB;
  
  @BeforeAll
  public void initialLoading()
      throws EnumByteExceedException, SqlColMetadataException, IOException, ClassNotFoundException {
    String dbDir = databaseDir.getAbsolutePath() + FILE_SEPARATOR + "clickhouse_mock";

    this.berkleyDB = new BerkleyDB(dbDir, true);

    Path resourceDirectory = Paths.get("src","test", "resources", "clickhouse");
    String absPath = resourceDirectory.toFile().getAbsolutePath();


    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(absPath).setBlockSize(16);
    FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    this.clickHouseMock = new ClickHouseMock();
    this.clickHouseMock.loadData(fStore, tableName);

    try {
      tProfile = fBase.getFStore().getTProfile(tableName);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }
    cProfiles = tProfile.getCProfiles();
  }

  @Test
  public void getRawResultBeginEndTest() throws IOException, ClassNotFoundException {

    Path resourceDirectory = Paths.get("src","test","resources","clickhouse");
    String absolutePath = resourceDirectory.toFile().getAbsolutePath();

    List<List<Object>> expected =
        (List<List<Object>>) getObject(absolutePath + FILE_SEPARATOR + "listsColStore.obj");
    List<List<Object>> actual =
        fStore.getRawDataAll(tProfile.getTableName(), Long.MIN_VALUE, Long.MAX_VALUE);

    log.info("expected:" + expected.get(0).size() + " actual:" + actual.size());

    assertEquals(expected.get(0).size(), actual.size());
  }

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
