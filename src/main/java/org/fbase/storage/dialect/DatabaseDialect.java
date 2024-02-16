package org.fbase.storage.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.fbase.model.profile.CProfile;

public interface DatabaseDialect {

  String getWhereClass(CProfile tsCProfile,
                       CProfile cProfileFilter,
                       String filter);

  void setDateTime(CProfile tsCProfile,
                   PreparedStatement ps,
                   int parameterIndex,
                   long dateTime) throws SQLException;
}
