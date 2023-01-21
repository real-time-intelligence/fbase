package org.fbase.service;

import java.util.List;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;

public interface MetadataService {

  List<Byte> getDataType(String tableName);

  List<Byte> getStorageType(String tableName);

  List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException;

  List<GanttColumn> getListGanttColumn(String tableName,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy,  long begin, long end) throws SqlColMetadataException;

  long getLastTimestamp(String tableName, long begin, long end);

}
