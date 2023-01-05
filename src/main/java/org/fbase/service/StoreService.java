package org.fbase.service;

import java.sql.ResultSet;
import java.util.List;
import org.fbase.model.profile.TProfile;

public interface StoreService {

  void putDataDirect(TProfile tProfile, List<List<Object>> data);

  long putDataJdbc(TProfile tProfile, ResultSet resultSet);

  void putDataBatch(TProfile tProfile, ResultSet resultSet, Integer fBaseBatchSize);

}
