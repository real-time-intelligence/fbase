package org.fbase.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.fbase.model.GroupFunction;
import org.fbase.model.profile.CProfile;

public interface DatabaseDialect {

  String getSelectClassGantt(CProfile firstCProfile, CProfile secondCProfile);

  String getSelectClassStacked(GroupFunction groupFunction, CProfile cProfile);

  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String filter);

  String getOrderByClass(CProfile tsCProfile);

  String getLimitClass(Integer fetchSize);

  void setDateTime(CProfile tsCProfile,
                   PreparedStatement ps,
                   int parameterIndex,
                   long dateTime) throws SQLException;
}
