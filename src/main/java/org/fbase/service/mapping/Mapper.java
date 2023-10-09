package org.fbase.service.mapping;

import static java.lang.String.valueOf;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;

@Log4j2
public class Mapper {
  public static int INT_NULL = Integer.MIN_VALUE;
  public static long LONG_NULL = Long.MIN_VALUE;
  public static float FLOAT_NULL = Float.MIN_VALUE;
  public static double DOUBLE_NULL = Double.MIN_VALUE;
  public static String STRING_NULL = "";

  public Mapper() {}

  public static CType isCType(CProfile cProfile) {
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING") ||
        cProfile.getColDbTypeName().contains("ENUM")) return CType.STRING;

    return switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case UINT8, UINT16, INT2, INT4, INT8, NUMBER, INTEGER, SMALLINT, INT, BIGINT, BIT, TIME, TIMETZ -> CType.INT;
      case OID, DATE, TIMESTAMP, TIMESTAMPTZ, DATETIME, UINT32, LONG, SERIAL, SMALLSERIAL, BIGSERIAL -> CType.LONG;
      case FLOAT4, FLOAT32 -> CType.FLOAT;
      case FLOAT64, DOUBLE, NUMERIC, FLOAT, FLOAT8, MONEY -> CType.DOUBLE;
      case BOOL, UUID, BYTEA -> CType.STRING;
      default -> CType.STRING;
    };
  }

  public static int convertRawToInt(Object obj, CProfile cProfile) {
    if (obj == null) return INT_NULL;
    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case UINT8:
      case UINT16:
      case INT2:
      case INT4:
      case INT8:
      case NUMBER:
      case INTEGER:
      case SMALLINT:
      case INT:
      case BIGINT:
      case TIME:
      case TIMETZ:
        if (obj instanceof BigDecimal bd) {
          return bd.intValue();
        } else if (obj instanceof Double db) {
          return db.intValue();
        } else if (obj instanceof Long lng) {
          return lng.intValue();
        } else if (obj instanceof Short sh) {
          return sh.intValue();
        } else if (obj instanceof Time t) {
          return Math.toIntExact(t.getTime());
        } else if (obj instanceof Float f) {
          return f.intValue();
        }
        return (Integer) obj;
      case BIT:
        if (obj instanceof Boolean b) {
          return b ? 1 : 0;
        }
      default:
        return INT_NULL;
    }
  }

  public static long convertRawToLong(Object obj, CProfile cProfile) {
    if (obj == null) return LONG_NULL;
    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
        if (obj instanceof Timestamp ts) {
          return ts.getTime();
        } else if (obj instanceof LocalDateTime localDateTime) {
          return localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } else if (obj instanceof LocalDate localDate) {
          return localDate.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        } else if (obj instanceof Date date) {
          return date.getTime();
        }
      case OID:
      case BIGSERIAL:
      case UINT32:
        return (Long) obj;
      case SERIAL:
      case SMALLSERIAL:
        if (obj instanceof Integer i) {
          return i;
        }
      default:
        return LONG_NULL;
    }
  }

  public static float convertRawToFloat(Object obj, CProfile cProfile) {
    if (obj == null) return FLOAT_NULL;
    if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.FLOAT32) {
      return (Float) obj;
    } else if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.FLOAT4) {
      return (Float) obj;
    }
    return FLOAT_NULL;
  }

  public static double convertRawToDouble(Object obj, CProfile cProfile) {
    if (obj == null) return DOUBLE_NULL;
    if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.FLOAT64) {
      return (Double) obj;
    } else if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.MONEY) {
      return (Double) obj;
    } else if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.FLOAT)  {
      if (obj instanceof BigDecimal bd) {
        return bd.doubleValue();
      } else {
        return (Double) obj;
      }
    } else if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.NUMERIC)  {
      if (obj instanceof BigDecimal bd) {
        return bd.doubleValue();
      } else {
        return (Double) obj;
      }
    } else if (DataType.valueOf(cProfile.getColDbTypeName()) == DataType.FLOAT8) {
      return (Double) obj;
    }
    return DOUBLE_NULL;
  }

  public static String convertRawToString(Object obj, CProfile cProfile) {
    if (obj == null) return STRING_NULL;
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")||
        cProfile.getColDbTypeName().contains("ENUM")) return (String) obj;

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case CHAR:
      case BPCHAR:
      case NAME:
      case TEXT:
      case VARCHAR:
      case VARCHAR2:
      case NVARCHAR:
        return (String) obj;
      case RAW:
        return getByte(obj);
      case OID:
        return valueOf(obj);
      case INT2:
      case INT4:
      case INT8:
        Integer int0 = (Integer) obj;
        return valueOf(int0.intValue());
      case FLOAT4:
      case FLOAT8:
        Double d = (Double) obj;
        return valueOf(d.intValue());
      case NUMBER:
        BigDecimal bgDec = (BigDecimal) obj;
        return valueOf(bgDec.longValue());
      case CLOB:
        Clob clobVal = (Clob) obj;
        try {
          return clobVal.getSubString(1, (int) clobVal.length());
        } catch (SQLException e) {
          log.info("No data found while clob processing");
          return "No clob data";
        }
      case BOOL:
        if (obj instanceof Boolean b) {
          return b.toString();
        }
      case UUID:
        if (obj instanceof UUID u) {
          return u.toString();
        }
      case BYTEA:
        return new String((byte[]) obj, StandardCharsets.UTF_8);
      default:
        return STRING_NULL;
    }
  }

  public static int getColumnCount(List<CProfile> cProfiles, Predicate<CProfile> isNotTimestamp,
      Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {

    return (int) cProfiles.stream()
        .filter(isNotTimestamp)
        .filter(isRaw)
        .filter(isCustom)
        .count();
  }

  public static int getColumnCount(List<CProfile> cProfiles, Map<Integer, SType> colIdSTypeMap,
      Predicate<CProfile> isNotTimestamp, Predicate<CProfile> isCustom) {

    return (int) cProfiles.stream()
        .filter(isNotTimestamp)
        .filter(f -> SType.RAW.equals(colIdSTypeMap.get(f.getColId())))
        .filter(isCustom)
        .count();
  }

  public static int getColumnCount(List<CProfile> cProfiles, Predicate<CProfile> isRaw, Predicate<CProfile> isCustom) {

    return (int) cProfiles.stream()
        .filter(isRaw)
        .filter(isCustom)
        .count();
  }

  private static String getByte(Object obj){
    Byte[] useValue;
    byte[] bytes = (byte[]) obj;
    useValue = new Byte[bytes.length];
    for (int m=0; m<bytes.length; m++) {
      useValue[m] = bytes[m];
    }
    return BinaryDisplayConverter.convertToString(useValue,
        BinaryDisplayConverter.HEX, false);
  }

}
