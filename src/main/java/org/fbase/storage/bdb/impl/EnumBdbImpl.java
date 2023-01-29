package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.util.Arrays;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.dictionary.EColumn;
import org.fbase.storage.helper.EnumHelper;

@Log4j2
public class EnumBdbImpl implements EnumDAO {
  private PrimaryIndex<ColumnKey, EColumn> primaryIndexEnumColumn;

  public EnumBdbImpl(EntityStore store) {
    this.primaryIndexEnumColumn = store.getPrimaryIndex(ColumnKey.class, EColumn.class);
  }

  @Override
  public int[] getEColumnValues(byte tableId, long key, int colIndex) {
    return this.primaryIndexEnumColumn.get(ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build()).getValues();
  }

  @Override
  public EColumn putEColumn(byte tableId, long key, int colIndex, int[] values) {
    ColumnKey columnKey = ColumnKey.builder().table(tableId).key(key).colIndex(colIndex).build();

    EColumn eColumn = new EColumn();
    eColumn.setColumnKey(columnKey);
    eColumn.setValues(values);

    this.primaryIndexEnumColumn.put(eColumn);

    return eColumn;
  }

}
