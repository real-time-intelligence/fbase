package org.fbase.model;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.source.H2Database;

@AllArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class FBaseTestConfig {
  private final FBaseConfig fBaseConfig;
  private final BerkleyDB berkleyDB;

  private final FBase fBase;
  private final FStore fStore;

  private final H2Database h2Db;
  private final Connection h2DbConnection;

  public FBaseTestConfig(File databaseDir, H2Database h2DbIn) throws IOException {
    // FBase initialization
    fBaseConfig = new FBaseConfig().setConfigDirectory(databaseDir.getAbsolutePath());
    berkleyDB = new BerkleyDB(databaseDir.getAbsolutePath(), true);
    fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();

    // Set H2Database and JDBC Connection
    h2Db = h2DbIn;
    h2DbConnection = h2DbIn.getConnection();
  }
}
