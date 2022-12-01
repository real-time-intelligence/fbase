package org.fbase;

import com.sleepycat.persist.EntityStore;
import lombok.Getter;
import org.fbase.config.FBaseConfig;
import org.fbase.core.BdbStore;
import org.fbase.core.FStore;

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
}
