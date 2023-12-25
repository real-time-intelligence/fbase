package org.fbase.util;

import java.util.Arrays;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MapArrayUtil {

  public static String arrayToString(Object obj) {
    if (obj instanceof long[] longArray) {
      return Arrays.toString(longArray);
    } else if (obj instanceof int[] intArray) {
      return Arrays.toString(intArray);
    } else if (obj instanceof short[] shortArray) {
      return Arrays.toString(shortArray);
    } else if (obj instanceof byte[] byteArray) {
      return Arrays.toString(byteArray);
    } else if (obj instanceof double[] doubleArray) {
      return Arrays.toString(doubleArray);
    } else if (obj instanceof String[] stringArray) {
      return Arrays.toString(stringArray);
    } else {
      throw new IllegalArgumentException("Object is not an array of a supported type.");
    }
  }
}
