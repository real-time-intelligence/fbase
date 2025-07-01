package org.fbase.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.profile.CProfile;

public interface DatabaseDialect {

  // Select class
  String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile);

  String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile);

  // Where class
  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String[] filterData,
                       CompareFunction compareFunction);

  // Order by class
  String getOrderByClass(CProfile tsCProfile);
  String getOrderByClass(CProfile cProfile, OrderBy orderBy);

  // Limit class
  String getLimitClass(Integer fetchSize);

  // Set date time for parameter
  void setDateTime(CProfile tsCProfile,
                   PreparedStatement ps,
                   int parameterIndex,
                   long dateTime) throws SQLException;
}
