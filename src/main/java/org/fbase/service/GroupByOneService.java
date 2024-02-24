package org.fbase.service;

import java.util.List;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;

public interface GroupByOneService {

  List<StackedColumn> getListStackedColumn(String tableName,
                                           CProfile cProfile,
                                           GroupFunction groupFunction,
                                           long begin,
                                           long end)
      throws SqlColMetadataException;

  List<StackedColumn> getListStackedColumnFilter(String tableName,
                                                 CProfile cProfile,
                                                 GroupFunction groupFunction,
                                                 CProfile cProfileFilter,
                                                 String filter,
                                                 long begin,
                                                 long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;
}
