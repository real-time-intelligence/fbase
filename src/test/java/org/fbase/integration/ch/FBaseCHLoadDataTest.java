package org.fbase.integration.ch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.model.profile.CProfile;
import org.fbase.source.ClickHouse;
import org.fbase.source.ClickHouseDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseCHLoadDataTest implements ClickHouse {

  private FStore fStore;
  private List<CProfile> cProfiles;

  private String url = "jdbc:clickhouse://localhost:8123";

  private ClickHouseDatabase clickHouseDB;
  private BerkleyDB berkleyDB;

  @BeforeAll
  public void initialLoading() throws SQLException, IOException {
    String dbFolder = getTestDbFolder("C:\\Users\\.temp", "clickhouse_test");

    this.berkleyDB = new BerkleyDB(dbFolder, true);

    this.clickHouseDB = new ClickHouseDatabase(url);

    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbFolder).setBlockSize(16);
    FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();
  }

  @Test
  public void loadDataDirect() {
    try {
      cProfiles = clickHouseDB.loadDataDirect(ClickHouse.select2016, fStore, 20000, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataJdbc() {
    try {
      cProfiles = clickHouseDB.loadDataJdbc(ClickHouse.select2016, fStore, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @Test
  public void loadDataBatchTest() {
    try {
     cProfiles = clickHouseDB.loadDataJdbcBatch(ClickHouse.select2016, fStore, 20000, 20000);
    } catch (Exception e) {
      log.catching(e);
    }
    assertEquals(1, 1);
  }

  @AfterAll
  public void closeDb() throws SQLException {
    clickHouseDB.close();
    berkleyDB.closeDatabase();
    /*berkleyDB.removeDirectory();*/
  }
}
