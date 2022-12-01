package org.fbase.core;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.DimensionDAO;

@Log4j2
public class Converter {

  private int intNullValue = Integer.MIN_VALUE;
  private double doubleNullValue = Double.MIN_VALUE;

  private DimensionDAO dimensionDAO;

  public Converter(DimensionDAO dimensionDAO) {
    this.dimensionDAO = dimensionDAO;
  }

  public double convertRawToDouble(Object obj, CProfile cProfile) {
    if (obj == null) return doubleNullValue;

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case FLOAT64:
        return (Double) obj;
      case FLOAT32:
        Float varF = (Float) obj;
        return varF.doubleValue();
      default:
        return doubleNullValue;
    }
  }

  public String convertDoubleToRaw(double objIndex, CProfile cProfile) {
    if (objIndex == doubleNullValue) return "";

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case FLOAT32:
      case FLOAT64:
      default:
        return String.valueOf(objIndex);
    }
  }

  public int convertRawToInt(Object obj, CProfile cProfile) {
    if (obj == null) return intNullValue;

    if (cProfile.getColDbTypeName().contains("ENUM")) return dimensionDAO.getOrLoad((String) obj);
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")) return dimensionDAO.getOrLoad((String) obj);

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case OID:
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
          return (int) ts.getTime() / 1000;
        } else if (obj instanceof LocalDateTime localDateTime) {
          return (int) localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() / 1000;
        }
      case UINT32:
        Long var = (Long) obj;
        return var.intValue();
      case UINT8:
      case UINT16:
      case INT4:
      case FLOAT8:
      case NUMBER:
      case INTEGER:
        if (obj instanceof BigDecimal bd) {
          return bd.intValue();
        } else if (obj instanceof Double db) {
          return db.intValue();
        } else if (obj instanceof Short sh) {
          return sh.intValue();
        }
        return (Integer) obj;
      case FLOAT64:
        Double varD = (Double) obj;
        return dimensionDAO.getOrLoad(varD);
      case FLOAT32:
        Float varF = (Float) obj;
        return dimensionDAO.getOrLoad(varF.doubleValue());
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case CHAR:
      case CLOB:
      case NAME:
      case TEXT:
      case VARCHAR:
      case VARCHAR2:
        return dimensionDAO.getOrLoad((String) obj);
      default:
        return intNullValue;
    }
  }

  public String convertIntToRaw(int objIndex, CProfile cProfile) {
    if (objIndex == intNullValue) return "";

    if (cProfile.getColDbTypeName().contains("ENUM")) return dimensionDAO.getStringById(objIndex);
    if (cProfile.getColDbTypeName().contains("FIXEDSTRING")) return dimensionDAO.getStringById(objIndex);

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
      case OID:
      case DATE:
      case ENUM8:
      case ENUM16:
      case FIXEDSTRING:
      case CHAR:
      case CLOB:
      case NAME:
      case TEXT:
      case VARCHAR:
      case VARCHAR2:
        return dimensionDAO.getStringById(objIndex);
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATETIME:
        return getDateForLongShorted(objIndex);
      case FLOAT64:
      case FLOAT32:
        return String.valueOf(dimensionDAO.getDoubleById(objIndex));
      default:
        return String.valueOf(objIndex);
    }
  }

  public long getKeyValue(Object obj, CProfile cProfile) {
    if (obj == null) {
      return 0L;
    }

    switch (DataType.valueOf(cProfile.getColDbTypeName())) {
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
}
