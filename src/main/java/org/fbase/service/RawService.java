package org.fbase.service;

import java.util.List;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.TProfile;

public interface RawService {

  List<StackedColumn> getListStackedColumn(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException;

  List<List<Object>> getRawDataAll(TProfile tProfile, long begin, long end);

  List<List<Object>> getRawDataByColumn(TProfile tProfile, CProfile cProfile, long begin, long end);

  List<List<Object>> getRawDataAll(TProfile tProfile);

}
