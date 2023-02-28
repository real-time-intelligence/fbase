package org.fbase.storage;

import org.fbase.storage.bdb.entity.dictionary.EColumn;

public interface EnumDAO {

  EColumn putEColumn(byte tableId, long blockId, int colId, int[] values);

  int[] getEColumnValues(byte tableId, long blockId, int colId);

}
