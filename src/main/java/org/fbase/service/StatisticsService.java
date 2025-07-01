package org.fbase.service;

import org.fbase.model.profile.cstype.SType;

public interface StatisticsService {

  boolean isStatByTableExist(byte tableId);
  SType getLastSType(byte tableId, int colId, boolean isTimestamp);
}
