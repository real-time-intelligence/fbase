package org.fbase.storage.helper;

import org.fbase.exception.EnumByteExceedException;
import org.fbase.util.CachedLastLinkedHashMap;

public class EnumHelper {

  public static byte getByteValue(CachedLastLinkedHashMap<Integer, Byte> values, int valueInt)
      throws EnumByteExceedException {
    if (values.isEmpty()) {
      values.put(valueInt, Byte.MIN_VALUE);
      return Byte.MIN_VALUE;
    }

    if (values.get(valueInt) != null) {
      return values.get(valueInt);
    } else {
      if (values.getLast() == Byte.MAX_VALUE) {
        throw new EnumByteExceedException("Exceed number of distinct values for column");
      }

      byte value = (byte) (values.getLast() + 1);
      values.put(valueInt, value);

      return value;
    }

  }

  public static int getIndexValue(int[] values, byte valueByte) {
    return values[valueByte < 0 ? (valueByte + 128) : valueByte];
  }

}
