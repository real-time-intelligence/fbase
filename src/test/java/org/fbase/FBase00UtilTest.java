package org.fbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.fbase.config.FBaseConfig;
import org.fbase.config.FileConfig;
import org.fbase.handler.MetaModelHandler;
import org.fbase.model.MetaModel;
import org.fbase.model.MetaModel.TableMetadata;
import org.fbase.model.profile.CProfile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class FBase00UtilTest {
  private MetaModel metaModel;

  @BeforeAll
  public void before() {
    metaModel = new MetaModel();

    List<CProfile> cProfileList = new ArrayList<>();
    metaModel.getMetadata().put("test1", new TableMetadata().setTableId((byte) (Byte.MIN_VALUE + 1)).setCProfiles(cProfileList));
    metaModel.getMetadata().put("test2", new TableMetadata().setTableId((byte) (1)).setCProfiles(cProfileList));
    metaModel.getMetadata().put("test3", new TableMetadata().setTableId((byte) (Byte.MAX_VALUE - 1)).setCProfiles(cProfileList));
  }

  @Test
  public void getNextInternalTableIdTest() {
    assertEquals(Byte.MAX_VALUE, MetaModelHandler.getNextInternalTableId(metaModel));
  }

  @Test
  public void metaModelSaveUpdateTest() throws IOException, ClassNotFoundException {
    Path resourceDirectory = Paths.get("src","test", "resources");
    String absPath = resourceDirectory.toFile().getAbsolutePath();

    FBaseConfig fBaseConfig = new FBaseConfig();
    fBaseConfig.setConfigDirectory(absPath);
    fBaseConfig.setConfigFileName("config.obj");

    // save
    FileConfig fileConfig = new FileConfig(fBaseConfig);
    fileConfig.saveObject(metaModel);

    MetaModel metaModelActualSave = (MetaModel) fileConfig.readObject();

    assertEquals(metaModel, metaModelActualSave);

    // update
    metaModelActualSave.getMetadata().put("test4", new TableMetadata().setTableId((byte) (1)).setCProfiles(new ArrayList<>()));
    fileConfig.saveObject(metaModelActualSave);

    MetaModel metaModelActualUpdated = (MetaModel) fileConfig.readObject();
    fileConfig.deleteFile();
  }

}
