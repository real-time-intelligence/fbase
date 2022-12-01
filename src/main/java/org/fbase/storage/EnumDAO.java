package org.fbase.storage;

import org.fbase.storage.bdb.entity.dictionary.EColumn;

public interface EnumDAO {

  int[] getEColumnValues(byte tableId, int colIndex);

  EColumn putEColumn(byte tableId, int colIndex);

  void updateEColumnValues(byte tableId, int colIndex, int[] values);
}
