package org.fbase;

import com.sleepycat.persist.EntityStore;
import lombok.Getter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.config.FBaseConfig;
import org.fbase.core.BdbStore;
import org.fbase.core.ChStore;
import org.fbase.core.FStore;
import org.fbase.core.PgSqlStore;
import org.fbase.model.profile.table.BType;

public class FBase {
  private final FBaseConfig fBaseConfig;
  private final EntityStore entityStore;

  @Getter
  private final FStore fStore;

  public FBase(FBaseConfig fBaseConfig, EntityStore entityStore) {
    this.fBaseConfig = fBaseConfig;
    this.entityStore = entityStore;

    this.fStore = new BdbStore(this.fBaseConfig, this.entityStore);
  }

  public FBase(FBaseConfig fBaseConfig, BType backendType, BasicDataSource basicDataSource) {
    this.fBaseConfig = fBaseConfig;
    this.entityStore = null;

    switch (backendType) {
      case CLICKHOUSE -> this.fStore = new ChStore(this.fBaseConfig, basicDataSource);
      case POSTGRES -> this.fStore = new PgSqlStore(this.fBaseConfig, basicDataSource);
      default -> throw new RuntimeException("Not supported yet for: " + backendType);
    }
  }
}
