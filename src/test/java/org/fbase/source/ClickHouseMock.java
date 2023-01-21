package org.fbase.source;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.fbase.core.FStore;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.profile.TProfile;

@Log4j2
public class ClickHouseMock implements ClickHouse {
  public final String FILE_SEPARATOR = System.getProperty("file.separator");

  public ClickHouseMock() {}

  public TProfile loadData(FStore fStore, String tableName)
      throws IOException, ClassNotFoundException, EnumByteExceedException, SqlColMetadataException {

    Path resourceDirectory = Paths.get("src","test","resources","clickhouse");

    String absolutePath = resourceDirectory.toFile().getAbsolutePath();

    List<List<Object>> listsColStore =
        (List<List<Object>>) getObject(absolutePath + FILE_SEPARATOR + "listsColStore.obj");

    TProfile tProfile;
    try {
      tProfile = fStore.getTProfile(tableName);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    fStore.putDataDirect(tProfile.getTableName(), listsColStore);

    return tProfile;
  }

}
