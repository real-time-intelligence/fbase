package org.fbase.common;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.source.JdbcSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractOracleTest implements JdbcSource {
  protected final String TEMP_DB_DIR = "C:\\Users\\.temp";
  protected final String BERKLEY_DB_DIR = Paths.get(TEMP_DB_DIR).toAbsolutePath().normalize() + FILE_SEPARATOR + "oracle_test";
  protected BerkleyDB berkleyDB;

  protected final String DB_URL = "jdbc:oracle:thin:@localhost:1523:orcl";
  protected Connection dbConnection;

  protected FBaseConfig fBaseConfig;
  protected FBase fBase;
  protected FStore fStore;

  @BeforeAll
  public void initBackendAndLoad() throws IOException {
    berkleyDB = new BerkleyDB(BERKLEY_DB_DIR, true);

    fBaseConfig = new FBaseConfig().setConfigDirectory(BERKLEY_DB_DIR).setBlockSize(16);
    fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    try {
      System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");

      dbConnection = DriverManager.getConnection(DB_URL, "system", "sys");
    } catch (SQLException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  protected SProfile getSProfileForRandom() {
    Map<String, CSType> csTypeMap = new HashMap<>();

    csTypeMap.put("DT", new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());

    csTypeMap.put("VALUE_HISTOGRAM", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("VALUE_ENUM", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("VALUE_RAW", new CSType().toBuilder().sType(SType.RAW).build());

    return new SProfile().setCsTypeMap(csTypeMap);
  }

  protected SProfile getSProfileForAsh(String select) throws SQLException {
    Map<String, CSType> csTypeMap = new HashMap<>();

    getSProfileForAsh(select, dbConnection).getCsTypeMap().forEach((key, value) -> {
      if (key.equals("SAMPLE_TIME")) {
        csTypeMap.put(key, new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());
      } else if (key.equals("EVENT")) {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.HISTOGRAM).build());
      } else {
        csTypeMap.put(key, new CSType().toBuilder().sType(SType.RAW).build());
      }
    });

    return new SProfile().setCsTypeMap(csTypeMap);
  }

  @AfterAll
  public void closeDb() throws IOException {
    berkleyDB.closeDatabase();
    berkleyDB.removeDirectory();
  }
}
