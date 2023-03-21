package org.fbase.source;

import static org.fbase.config.FileConfig.FILE_SEPARATOR;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

public interface ClickHouse {
  String tableName = "ch_table_test";
  String select2016 = "SELECT * FROM datasets.trips_mergetree where toYear(pickup_date) = 2016";

  default Object getObject(String fullFileName) throws IOException, ClassNotFoundException {
    File f = new File(fullFileName);
    FileInputStream fis = new FileInputStream(f);
    ObjectInputStream ois = new ObjectInputStream(fis);
    Object out = ois.readObject();

    ois.close();
    fis.close();

    return out;
  }

  
  default SType getSType(String colName) {
    if (colName.equalsIgnoreCase("PICKUP_DATE") |
        colName.equalsIgnoreCase("DROPOFF_DATE") |
        colName.equalsIgnoreCase("STORE_AND_FWD_FLAG") |
        colName.equalsIgnoreCase("RATE_CODE_ID") |
        colName.equalsIgnoreCase("EXTRA") |
        colName.equalsIgnoreCase("MTA_TAX") |
        colName.equalsIgnoreCase("TIP_AMOUNT") |
        colName.equalsIgnoreCase("TOLLS_AMOUNT") |
        colName.equalsIgnoreCase("EHAIL_FEE") |
        colName.equalsIgnoreCase("IMPROVEMENT_SURCHARGE") |
        colName.equalsIgnoreCase("TRIP_TYPE") |
        colName.equalsIgnoreCase("PICKUP_BOROCODE") |
        colName.equalsIgnoreCase("PICKUP_BORONAME")
    ) {
      return SType.HISTOGRAM;
    } else if (colName.equalsIgnoreCase("PAYMENT_TYPE_") |
        colName.equalsIgnoreCase("CAB_TYPE") |

        colName.equalsIgnoreCase("DROPOFF_NTACODE") |
        colName.equalsIgnoreCase("DROPOFF_NTANAME") |
        colName.equalsIgnoreCase("PICKUP_NTACODE") |
        colName.equalsIgnoreCase("PICKUP_NTANAME") |
        colName.equalsIgnoreCase("PASSENGER_COUNT") |
        colName.equalsIgnoreCase("DROPOFF_PUMA") |
        colName.equalsIgnoreCase("DROPOFF_BOROCODE") |
        colName.equalsIgnoreCase("DROPOFF_BORONAME") |
        colName.equalsIgnoreCase("DROPOFF_CDELIGIBIL")
    ) {
      return SType.ENUM;
    } else {
      return SType.RAW;
    }
  }
  
  default SProfile getSProfile(String tableName, TType tType, IType iType, Boolean compression) {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(tType);
    sProfile.setIndexType(iType);
    sProfile.setCompression(compression);
    Map<String, CSType> csTypeMap = new HashMap<>();

    csTypeMap.put("PICKUP_DATETIME", new CSType().toBuilder().isTimeStamp(true).sType(SType.RAW).build());

    csTypeMap.put("PICKUP_DATE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("DROPOFF_DATE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("STORE_AND_FWD_FLAG", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("RATE_CODE_ID", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("EXTRA", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("MTA_TAX", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("TIP_AMOUNT", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("TOLLS_AMOUNT", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("EHAIL_FEE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("IMPROVEMENT_SURCHARGE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("TRIP_TYPE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("PICKUP_BOROCODE", new CSType().toBuilder().sType(SType.HISTOGRAM).build());
    csTypeMap.put("PICKUP_BORONAME", new CSType().toBuilder().sType(SType.HISTOGRAM).build());

    csTypeMap.put("VENDOR_ID", new CSType().toBuilder().sType(SType.RAW).build());
    csTypeMap.put("PICKUP_CDELIGIBIL", new CSType().toBuilder().sType(SType.RAW).build());

    csTypeMap.put("PAYMENT_TYPE_", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("CAB_TYPE", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_NTACODE", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_NTANAME", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("PICKUP_NTACODE", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("PICKUP_NTANAME", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("PASSENGER_COUNT", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_PUMA", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_BOROCODE", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_BORONAME", new CSType().toBuilder().sType(SType.ENUM).build());
    csTypeMap.put("DROPOFF_CDELIGIBIL", new CSType().toBuilder().sType(SType.ENUM).build());
    
    sProfile.setCsTypeMap(csTypeMap);
    
    return sProfile;
  }
  default String getTestDbFolder(String rootFolder, String folderName) {
    return String.format("%s%s" + folderName, Paths.get(rootFolder).toAbsolutePath().normalize(), FILE_SEPARATOR);
  }
}
