package org.fbase.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FileConfig {

  public static final String FILE_SEPARATOR = System.getProperty("file.separator");
  private final FBaseConfig fBaseConfig;

  public FileConfig(FBaseConfig fBaseConfig) {
    this.fBaseConfig = fBaseConfig;
  }

  public void saveObject(Object obj) throws IOException {
    File f = new File(getPathName());
    FileOutputStream fos = new FileOutputStream(f);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(obj);
    oos.close();
    fos.close();
  }

  public Object readObject() throws IOException, ClassNotFoundException {
    File f = new File(getPathName());
    if (!f.exists()) {
      return null;
    }

    FileInputStream fis = new FileInputStream(f);
    ObjectInputStream ois = new ObjectInputStream(fis);
    Object obj = ois.readObject();
    ois.close();
    fis.close();

    return obj;
  }

  public void deleteFile() {
    File file = new File(getPathName());
    if (file.delete()) {
      log.info("File " + getPathName() + " deleted successfully");
    } else {
      log.info("Failed to delete the file " + getPathName());
    }
  }

  private String getPathName() {
    Path resourceDirectory = Paths.get(fBaseConfig.getConfigDirectory());
    String absolutePath = resourceDirectory.toFile().getAbsolutePath();
    return absolutePath + FILE_SEPARATOR + fBaseConfig.getConfigFileName();
  }
}
