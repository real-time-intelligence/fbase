package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.column.EColumn;

@Log4j2
public class EnumBdbImpl implements EnumDAO {
  private PrimaryIndex<ColumnKey, EColumn> primaryIndexEnumColumn;

  public EnumBdbImpl(EntityStore store) {
    this.primaryIndexEnumColumn = store.getPrimaryIndex(ColumnKey.class, EColumn.class);
  }

  @Override
  public int[] getEColumnValues(byte tableId, long blockId, int colId) {
    return this.primaryIndexEnumColumn.get(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build()).getValues();
  }

  @Override
  public EColumn putEColumn(byte tableId, long blockId, int colId, int[] values) {
    ColumnKey columnKey = ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build();

    EColumn eColumn = new EColumn();
    eColumn.setColumnKey(columnKey);
    eColumn.setValues(values);

    this.primaryIndexEnumColumn.put(eColumn);

    return eColumn;
  }

}
