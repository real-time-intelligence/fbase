package org.fbase.service;

import java.util.List;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.TProfile;

public interface MetadataService {

  List<Byte> getDataType(TProfile tProfile);

  List<Byte> getStorageType(TProfile tProfile);

  List<StackedColumn> getListStackedColumn(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException;

  List<GanttColumn> getListGanttColumn(TProfile tProfile,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy,  long begin, long end) throws SqlColMetadataException;

  long getLastTimestamp(TProfile tProfile, long begin, long end);

}
