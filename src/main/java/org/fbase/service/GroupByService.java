package org.fbase.service;

import java.util.List;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.CompareFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.GanttColumnSum;
import org.fbase.model.profile.CProfile;

public interface GroupByService {

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            long begin,
                                            long end) throws SqlColMetadataException;

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            int batchSize,
                                            long begin,
                                            long end) throws SqlColMetadataException, BeginEndWrongOrderException;

  List<GanttColumnCount> getListGanttColumn(String tableName,
                                            CProfile firstGrpBy,
                                            CProfile secondGrpBy,
                                            CProfile cProfileFilter,
                                            String[] filterData,
                                            CompareFunction compareFunction,
                                            long begin,
                                            long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  List<GanttColumnSum> getListGanttSumColumn(String tableName,
                                             CProfile firstGrpBy,
                                             CProfile secondGrpBy,
                                             long begin,
                                             long end) throws SqlColMetadataException;

  List<GanttColumnSum> getListGanttSumColumn(String tableName,
                                             CProfile firstGrpBy,
                                             CProfile secondGrpBy,
                                             CProfile cProfileFilter,
                                             String[] filterData,
                                             CompareFunction compareFunction,
                                             long begin,
                                             long end) throws SqlColMetadataException;

  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end);

  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end,
                           CProfile cProfileFilter,
                           String[] filterData,
                           CompareFunction compareFunction);
}
