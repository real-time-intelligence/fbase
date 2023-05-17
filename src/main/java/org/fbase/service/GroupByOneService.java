package org.fbase.service;

import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;

import java.util.List;

public interface GroupByOneService {

  List<StackedColumn> getListStackedColumn(String tableName, CProfile cProfile, long begin, long end)
          throws SqlColMetadataException;
}
