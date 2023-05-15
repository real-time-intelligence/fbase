package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import java.io.IOException;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.CompressType;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.bdb.QueryBdbApi;
import org.fbase.storage.bdb.entity.ColumnKey;
import org.fbase.storage.bdb.entity.column.EColumn;
import org.xerial.snappy.Snappy;

@Log4j2
public class EnumBdbImpl extends QueryBdbApi implements EnumDAO {
  private final PrimaryIndex<ColumnKey, EColumn> primaryIndexEnumColumn;

  public EnumBdbImpl(EntityStore store) {
    this.primaryIndexEnumColumn = store.getPrimaryIndex(ColumnKey.class, EColumn.class);
  }

  @Override
  public EColumn putEColumn(byte tableId, long blockId, int colId, int[] values, byte[] data, boolean compression)
      throws IOException {
    ColumnKey columnKey = ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build();

    EColumn eColumn = new EColumn();
    eColumn.setColumnKey(columnKey);
    eColumn.setCompressionType(compression ? CompressType.BYTE : CompressType.NONE);
    eColumn.setValues(values);
    eColumn.setDataByte(compression ? Snappy.compress(data) : data);

    this.primaryIndexEnumColumn.put(eColumn);

    return eColumn;
  }

  @Override
  public EColumn getEColumnValues(byte tableId, long blockId, int colId) {
    EColumn eColumn =
        this.primaryIndexEnumColumn.get(ColumnKey.builder().tableId(tableId).blockId(blockId).colId(colId).build());

    if (eColumn == null) {
      log.info("No data found for t::b::c -> " + tableId + "::" + blockId + "::" + colId);
    }

    if (isNotBlockCompressed(eColumn)) {
      return eColumn;
    }

    byte[] unCompressed;
    try {
      unCompressed = Snappy.uncompress(eColumn.getDataByte());
    } catch (IOException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
    eColumn.setDataByte(unCompressed);

    return eColumn;
  }

}
