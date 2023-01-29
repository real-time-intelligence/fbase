package org.fbase.storage;

import org.fbase.storage.bdb.entity.dictionary.EColumn;

public interface EnumDAO {

  EColumn putEColumn(byte tableId, long key, int colIndex, int[] values);

  int[] getEColumnValues(byte tableId, long key, int colIndex);

}
