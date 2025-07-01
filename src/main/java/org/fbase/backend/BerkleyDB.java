package org.fbase.backend;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import lombok.Getter;
import lombok.Setter;

public class BerkleyDB {

  @Getter
  @Setter
  private String directory;
  @Getter
  private String storeName;
  @Getter
  private EnvironmentConfig envConfig;
  @Getter
  private Environment env;
  @Getter
  private StoreConfig storeConfig;
  @Getter
  private EntityStore store;

  public BerkleyDB(String directory, boolean cleanDirectory) throws IOException {
    this(directory, "FBase.db", cleanDirectory);
  }

  public BerkleyDB(String directory, String storeName, boolean cleanDirectory) throws IOException {
    this.directory = directory;
    this.storeName = storeName;

    if (cleanDirectory) {
      this.cleanDirectory();
    }

    this.createDirectory();
    this.setupEnvConfig();
    this.setupEnvironment();
    this.setupStoreConfig();
  }

  public void createDirectory() throws IOException {
    if (!Files.exists(Path.of(directory))) {
      Files.createDirectories(Path.of(directory));
    }
  }

  public void cleanDirectory() throws IOException {
    if (Files.exists(Path.of(directory))) {
      Files.walk(Path.of(directory))
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  private void setupEnvConfig() {
    this.envConfig = new EnvironmentConfig();
    this.envConfig.setAllowCreate(true);
    this.envConfig.setTransactional(false);
    this.envConfig.setCachePercent(50);
  }

  private void setupEnvironment() {
    this.env = new Environment(new File(this.directory), envConfig);
  }

  private void setupStoreConfig() {
    this.storeConfig = new StoreConfig();
    this.storeConfig.setAllowCreate(true);
    this.storeConfig.setTransactional(false);
    this.storeConfig.setDeferredWrite(true);

    this.store = new EntityStore(this.env, this.storeName, this.storeConfig);
  }

  public void closeDatabase() {
    if (this.store != null) {
      this.store.close();
    }
    if (this.env != null) {
      this.env.close();
    }
  }

  public void removeDirectory() throws IOException {
    this.closeDatabase();
    Files.walk(Path.of(directory))
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }
}