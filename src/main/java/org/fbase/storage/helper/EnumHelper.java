package org.fbase.storage.helper;

import java.util.stream.IntStream;
import org.fbase.exception.EnumByteExceedException;

public class EnumHelper {
  public static int ENUM_INDEX_CAPACITY = 255;

  public static int getByteValue(int[] values, int valueInt) throws EnumByteExceedException {
    int valueByte = indexOf(values, valueInt);

    if (valueByte == Integer.MIN_VALUE) {
      int setValue = indexOf(values, Integer.MAX_VALUE);

      if (setValue == Integer.MIN_VALUE) {
        throw new EnumByteExceedException("Exceed number of distinct values for column");
      }

      values[setValue] = valueInt;

      if (setValue > 127) {
        valueByte = -(setValue - 127);
      } else {
        valueByte = setValue;
      }
    } else {
      if (valueByte > 127) {
        valueByte = -(valueByte - 127);
      }
    }

    return valueByte;
  }

  public static int getIndexValue(int[] values, byte valueByte) {
    return values[valueByte < 0 ? (-valueByte + 127) : valueByte];
  }

  public static int indexOf(int[] arr, int value) {
    return IntStream.range(0, arr.length).filter(i -> arr[i] == value).findFirst().orElse(Integer.MIN_VALUE);
  }

}
