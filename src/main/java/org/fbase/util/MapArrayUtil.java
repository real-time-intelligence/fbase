package org.fbase.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  @FunctionalInterface
  public interface KeyParser<K> {
    K parse(String input);
  }

  @FunctionalInterface
  public interface ValueParser<V> {
    V parse(String input);
  }

  public static <K, V> Map<K, V> parseStringToTypedMap(String input, KeyParser<K> keyParser, ValueParser<V> valueParser, String KVDelimiter) {
    Map<K, V> map = new HashMap<>();
    Pattern p = Pattern.compile("([\\w]+)" + KVDelimiter + "([0-9]+\\.?[0-9]*)");
    Matcher m = p.matcher(input);

    while (m.find()) {
      K key = keyParser.parse(m.group(1));
      V value = valueParser.parse(m.group(2));
      map.put(key, value);
    }

    return map;
  }

  public static String[] parseStringToTypedArray(String input, String d) {
    String dsv = input.replace("[", "").replace("]", "");

    return dsv.split(d);
  }

  public static String mapToJson(Map<?, ?> map) {
    StringJoiner sj = new StringJoiner(",", "{", "}");
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());
      String value = toJsonValue(entry.getValue());
      sj.add(toJsonString(key) + ":" + value);
    }
    return sj.toString();
  }

  public static String mapToJsonRaw(Map<?, ?> map) {
    StringJoiner sj_map = new StringJoiner(", ", "{", "}");
    map.forEach((key, value) -> sj_map.add("'" + key + "':" + value));
    return sj_map.toString();
  }

  private static String toJsonValue(Object value) {
    if (value instanceof Map<?, ?>) {
      return mapToJson((Map<?, ?>) value);
    } else if (value instanceof String) {
      return toJsonString((String) value);
    } else if (value instanceof Number) {
      return value.toString();
    } else if (value instanceof Boolean) {
      return value.toString();
    } else if (value == null) {
      return "null";
    } else {
      return toJsonString(value.toString());
    }
  }

  private static String toJsonString(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
