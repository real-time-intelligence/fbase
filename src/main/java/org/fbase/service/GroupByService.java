package org.fbase.service;

import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;

import java.util.List;

public interface GroupByService {

  List<GanttColumn> getListGanttColumnUniversal(TProfile tProfile,
                                                CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end) throws SqlColMetadataException;

}
