package org.fbase.service;

import java.util.List;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;

public interface GroupByService {

  List<GanttColumn> getListGanttColumnUniversal(String tableName, CProfile firstLevelGroupBy, CProfile secondLevelGroupBy,
      long begin, long end) throws SqlColMetadataException;
}
