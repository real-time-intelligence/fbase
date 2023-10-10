package org.fbase.storage;

import static org.fbase.service.mapping.Mapper.INT_NULL;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;
import org.fbase.service.mapping.BinaryDisplayConverter;

@Log4j2
public class Converter {

  private final DimensionDAO dimensionDAO;

  public Converter(DimensionDAO dimensionDAO) {
    this.dimensionDAO = dimensionDAO;
  }

  public int convertRawToInt(Object obj, CProfile cProfile) {
    if (obj == null) return INT_NULL;

    if (cProfile.getColDbTypeName().contains("ENUM")) return dimensionDAO.getOrLoad((String) obj);
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")) return dimensionDAO.getOrLoad((String) obj);

    switch (DataType.valueOf(cProfile.getColDbTypeName().replaceAll(" ", "_").toUpperCase())) {
      case DATE:
        if (obj instanceof java.util.Date dt) {
          return dimensionDAO.getOrLoad(dt.toString());
        } else if (obj instanceof LocalDateTime localDateTime) {
          return dimensionDAO.getOrLoad(localDateTime.toString());
        } else if (obj instanceof LocalDate localDate) {
          return dimensionDAO.getOrLoad(localDate.toString());
        }
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
        if (obj instanceof Timestamp ts) {
          return Math.toIntExact(ts.getTime() / 1000);
        } else if (obj instanceof LocalDateTime localDateTime) {
          return Math.toIntExact(localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() / 1000);
        }
      case OID:
      case UINT32:
      case SERIAL:
      case SMALLSERIAL:
      case BIGSERIAL:
        Long l = (Long) obj;
        return l.intValue();
      case UINT8:
      case UINT16:
      case INT2:
      case INT4:
      case INT8:
      case NUMBER:
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case INT:
      case BIT:
      case TIME:
      case TIMETZ:
        if (obj instanceof BigDecimal bd) {
          return bd.intValue();
        } else if (obj instanceof Double db) {
          return db.intValue();
        } else if (obj instanceof Time db) {
          return Math.toIntExact(db.getTime());
        } else if (obj instanceof Short sh) {
          return sh.intValue();
        }
        return (Integer) obj;
      case FLOAT64:
      case FLOAT:
      case NUMERIC:
      case MONEY:
        if (obj instanceof BigDecimal bd) {
          return dimensionDAO.getOrLoad(bd.doubleValue());
        } else {
          Double varD = (Double) obj;
          return dimensionDAO.getOrLoad(varD);
        }
      case FLOAT8:
        Double d = (Double) obj;
        return dimensionDAO.getOrLoad(d);
      case FLOAT4:
      case FLOAT32:
        Float f = (Float) obj;
        return dimensionDAO.getOrLoad(f.doubleValue());
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case CHAR:
      case NCLOB:
      case NCHAR:
      case BPCHAR:
      case CLOB:
      case NAME:
      case TEXT:
      case VARCHAR:
      case NVARCHAR2:
      case VARCHAR2:
      case NVARCHAR:
        return dimensionDAO.getOrLoad((String) obj);
      /*case RAW:
        return dimensionDAO.getOrLoad(getByte(obj));*/
      default:
        return INT_NULL;
    }
  }

  public String convertIntToRaw(int objIndex, CProfile cProfile) {
    if (objIndex == INT_NULL) return "";

    if (cProfile.getColDbTypeName().contains("ENUM")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")) return dimensionDAO.getStringById(objIndex);

    return switch (DataType.valueOf(cProfile.getColDbTypeName().replaceAll(" ", "_").toUpperCase())) {
      case DATE, ENUM8, ENUM16, FIXEDSTRING, CHAR, NCHAR, NCLOB, CLOB, NAME, TEXT, VARCHAR, NVARCHAR2, VARCHAR2, NVARCHAR ->
          dimensionDAO.getStringById(objIndex);
      case TIMESTAMP, TIMESTAMPTZ, DATETIME -> getDateForLongShorted(objIndex);
      case FLOAT64, FLOAT4, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY -> String.valueOf(dimensionDAO.getDoubleById(objIndex));
      default -> String.valueOf(objIndex);
    };
  }

  public double convertIntToDouble(int objIndex, CProfile cProfile) {
    return switch (DataType.valueOf(cProfile.getColDbTypeName().replaceAll(" ", "_").toUpperCase())) {
      case FLOAT64, FLOAT4, FLOAT8, FLOAT32, FLOAT, NUMERIC, MONEY  -> dimensionDAO.getDoubleById(objIndex);
      default -> objIndex;
    };
  }

  public long getKeyValue(Object obj, CProfile cProfile) {
    if (obj == null) {
      return 0L;
    }

    switch (DataType.valueOf(cProfile.getColDbTypeName().replaceAll(" ", "_").toUpperCase())) {
      case INTEGER:
        return ((Integer) obj).longValue();
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
        }
      default:
        return 0L;
    }
  }

  private String getDateForLongShorted(int longDate) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    Date dtDate= new Date(((long)longDate)*1000L);
    return simpleDateFormat.format(dtDate);
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
