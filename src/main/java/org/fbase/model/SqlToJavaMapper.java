package org.fbase.model;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.profile.cstype.CType;

@Log4j2
public class SqlToJavaMapper {

  private final List<String> byteList = Arrays.asList("BYTE");
  private final List<String> intList = Arrays.asList("INTEGER", "UINT8", "UINT16");
  private final List<String> longList = Arrays.asList("LONG", "UINT32", "DATETIME");
  private final List<String> floatList = Arrays.asList("FLOAT32");
  private final List<String> doubleList = Arrays.asList("FLOAT64", "DOUBLE");
  private final List<String> stringList = Arrays.asList("CHAR", "VARCHAR", "VARCHAR2", "FIXEDSTRING", "ENUM16");

  public SqlToJavaMapper() {}

  public CType getJavaType(String colDbTypeName) throws SqlColMetadataException {
    if (isContain(byteList, colDbTypeName.toUpperCase())) {
      return CType.BYTE;
    } else if (isContain(intList, colDbTypeName.toUpperCase())) {
      return CType.INT;
    } else if (isContain(longList, colDbTypeName.toUpperCase())) {
      return CType.LONG;
    } else if (isContain(floatList, colDbTypeName.toUpperCase())) {
      return CType.FLOAT;
    } else if (isContain(doubleList, colDbTypeName.toUpperCase())) {
      return CType.DOUBLE;
    } else if (isContain(stringList, colDbTypeName.toUpperCase())) {
      return CType.STRING;
    } else {
      throw new SqlColMetadataException(
          "Not found java mapping for sql type:" + colDbTypeName + " ..");
    }

  }

  private boolean isContain(List<String> source, String item) {
    AtomicBoolean isContain = new AtomicBoolean(false);
    String pattern = "\\b" + item + "\\b";
    Pattern p = Pattern.compile(pattern);

    source.stream().filter(e -> p.matcher(e).find())
        .findAny().ifPresentOrElse(
            (value) -> isContain.set(true), () -> isContain.set(false));

    return isContain.get();
  }
}
