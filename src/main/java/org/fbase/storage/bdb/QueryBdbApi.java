package org.fbase.storage.bdb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityIndex;
import java.util.List;
import org.fbase.metadata.CompressType;
import org.fbase.storage.bdb.entity.column.HColumn;
import org.fbase.storage.bdb.entity.column.RColumn;

public abstract class QueryBdbApi {

  public <K, V> EntityCursor<V> doRangeQuery(EntityIndex<K, V> index,
      K fromKey,
      boolean fromInclusive,
      K toKey,
      boolean toInclusive)
      throws DatabaseException {

    assert (index != null);

    return index.entities(fromKey,
        fromInclusive,
        toKey,
        toInclusive);
  }

  public boolean isNotBlockCompressed(RColumn rColumn) {
    return rColumn.getCompressionType() == null || CompressType.NONE.equals(rColumn.getCompressionType());
  }

  public boolean isNotBlockCompressed(HColumn HColumn) {
    return HColumn.getCompressionType() == null || CompressType.NONE.equals(HColumn.getCompressionType());
  }

  public static float[] convertDoubleArrayToFloatArray(double[] input) {
    float[] result = new float[input.length];
    for (int i = 0; i < input.length; i++) {
      result[i] = (float) input[i];
    }
    return result;
  }

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }
    return byteArray;
  }

}
