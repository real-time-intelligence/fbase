package org.fbase.storage.helper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
