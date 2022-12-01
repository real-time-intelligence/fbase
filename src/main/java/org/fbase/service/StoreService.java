package org.fbase.service;

import java.sql.ResultSet;
import java.util.List;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.model.profile.TProfile;

public interface StoreService {

  void putData(TProfile tProfile, List<List<Object>> data);

  long putDataDirect(TProfile tProfile, ResultSet resultSet);

  void putDataBatch(TProfile tProfile, ResultSet resultSet, Integer fBaseBatchSize);

}
