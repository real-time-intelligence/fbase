package org.fbase.storage.helper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

@UtilityClass
@Log4j2
public class ClickHouseHelper {

  public static boolean checkUnsigned(String className) {
    return switch (className) {
      case "com.clickhouse.data.value.UnsignedByte",
          "com.clickhouse.data.value.UnsignedInteger",
          "com.clickhouse.data.value.UnsignedLong",
          "com.clickhouse.data.value.UnsignedShort" -> true;
      default -> false;
    };
  }

  public static String getDateTime(long unixTimestamp) {
    return Instant.ofEpochMilli(unixTimestamp).atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  public static Map<String, Integer> enumParser(String enumDescription) {
     Map<String, Integer> enumMap = new HashMap<>();

    String cleanEnum = enumDescription.substring(enumDescription.indexOf('(') + 1, enumDescription.length() - 1);

    String[] pairs = cleanEnum.split(", ");

    for (String pair : pairs) {
      String[] keyValue = pair.split(" = ");
      String key = keyValue[0].substring(1, keyValue[0].length() - 1);
      Integer value = Integer.parseInt(keyValue[1]);
      enumMap.put(key, value);
    }

     return enumMap;
  }

  public static <T> T invokeMethod(Object obj,
                                   String method,
                                   Class<T> returnType) {
    Method m;
    try {
      m = obj.getClass().getMethod(method);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("No such method: " + method, e);
    }
    try {
      Object result = m.invoke(obj);

      if (!returnType.isInstance(result)) {
        throw new RuntimeException(
            "Expected return type " + returnType.getName() + ", but got " + result.getClass().getName());
      }

      return returnType.cast(result);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Method invocation failed: " + method, e);
    }
  }
}
