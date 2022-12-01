package org.fbase.service;

import java.util.List;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.TProfile;

public interface EnumService {

  List<StackedColumn> getListStackedColumn(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException;
}
