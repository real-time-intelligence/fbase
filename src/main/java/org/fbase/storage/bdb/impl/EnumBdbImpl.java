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
  public int[] getEColumnValues(byte tableId, int colIndex) {
    return this.primaryIndexEnumColumn.get(ColumnKey.builder().table(tableId).key(Long.MIN_VALUE).colIndex(colIndex).build()).getValues();
  }

  @Override
  public EColumn putEColumn(byte tableId, int colIndex) {
    ColumnKey columnKey = ColumnKey.builder().table(tableId).key(Long.MIN_VALUE).colIndex(colIndex).build();

    if (this.primaryIndexEnumColumn.contains(columnKey)) {
      return this.primaryIndexEnumColumn.get(columnKey);
    } else {
      int[] values = new int[EnumHelper.ENUM_INDEX_CAPACITY];
      Arrays.fill(values, Integer.MAX_VALUE);

      EColumn eColumn = new EColumn();
      eColumn.setColumnKey(columnKey);
      eColumn.setValues(values);

      this.primaryIndexEnumColumn.put(eColumn);

      return eColumn;
    }
  }

  @Override
  public void updateEColumnValues(byte tableId, int colIndex, int[] values) {
    ColumnKey columnKey = ColumnKey.builder().table(tableId).key(Long.MIN_VALUE).colIndex(colIndex).build();

    EColumn eColumn = this.primaryIndexEnumColumn.get(columnKey);

    if (EnumHelper.indexOf(eColumn.getValues(),Integer.MAX_VALUE) != EnumHelper.indexOf(values,Integer.MAX_VALUE)) {
      eColumn.setValues(values);
      this.primaryIndexEnumColumn.put(eColumn);
    } else {
      //log.info("No updates for EColumn with colIndex= " + colIndex);
    }
  }

}
