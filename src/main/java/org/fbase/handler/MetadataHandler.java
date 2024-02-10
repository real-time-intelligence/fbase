package org.fbase.handler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import org.fbase.metadata.DataType;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;

@Log4j2
public class MetadataHandler {

  public static List<CProfile> getDirectCProfileList(SProfile sProfile) {
    List<CProfile> cProfileList = new ArrayList<>();

    AtomicInteger counter = new AtomicInteger(0);

    sProfile.getCsTypeMap().forEach((columName, csType) ->
        cProfileList.add(CProfile.builder()
            .colId(counter.getAndAdd(1))
            .colIdSql(counter.get())
            .colDbTypeName(csType.getCType().name().toUpperCase())
            .colName(columName)
            .csType(CSType.builder()
                .isTimeStamp(csType.isTimeStamp())
                .sType(csType.getSType())
                .cType(csType.getCType())
                .dType(csType.getDType())
                .build())
            .build()));

    return cProfileList;
  }

  public static List<CProfile> getJdbcCProfileList(Connection connection,
                                                   String select) throws SQLException {
    List<CProfile> cProfileList = new ArrayList<>();

    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;

    s = connection.createStatement();
    s.executeQuery(select);
    rs = s.getResultSet();
    rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      cProfileList.add(i - 1,
          CProfile.builder()
              .colId(i - 1)
              .colIdSql(i)
              .colName(rsmd.getColumnName(i).toUpperCase())
              .colDbTypeName(rsmd.getColumnTypeName(i).toUpperCase())
              .colSizeDisplay(rsmd.getColumnDisplaySize(i))
              .colSizeSqlType(rsmd.getColumnType(i))
              .build());
    }

    rs.close();
    s.close();

    return cProfileList;
  }

  public static List<CProfile> getCsvCProfileList(SProfile sProfile) {
    List<CProfile> cProfileList = new ArrayList<>();

    AtomicInteger counter = new AtomicInteger(0);

    sProfile.getCsTypeMap().forEach((k, csType) ->
        cProfileList.add(CProfile.builder()
            .colId(counter.getAndAdd(1))
            .colDbTypeName(csType.getCType().name().toUpperCase())
            .colName(k)
            .csType(CSType.builder()
                .sType(csType.getSType())
                .cType(csType.getCType())
                .dType(csType.getDType())
                .build())
            .build()));

    return cProfileList;
  }

  public static void loadMetadataFromCsv(String csvFile,
                                         String csvSplitBy,
                                         SProfile sProfile) {
    String line = "";
    Map<String, CSType> csTypeMapSorted = new LinkedHashMap<>();

    Map<Map.Entry<Integer, String>, CSType> csTypeMapEntry = new HashMap<>();

    try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
      line = br.readLine();
      String[] headers = line.split(csvSplitBy);
      log.info("Header = " + Arrays.toString(headers));

      line = br.readLine();
      String[] data = line.split(csvSplitBy);
      log.info("Data (1 row) = " + Arrays.toString(data));

      for (int i = 0; i < headers.length; i++) {
        String header = headers[i];
        String colData = data[i];

        if (isParsableAsLong(colData)) {
          csTypeMapEntry.put(Map.entry(i, header), CSType.builder()
              .sType(SType.RAW)
              .cType(CType.LONG)
              .dType(DataType.LONG)
              .build());
        } else if (isParsableAsDouble(colData)) {
          csTypeMapEntry.put(Map.entry(i, header), CSType.builder()
              .sType(SType.RAW)
              .cType(CType.DOUBLE)
              .dType(DataType.DOUBLE)
              .build());
        } else {
          csTypeMapEntry.put(Map.entry(i, header), CSType.builder()
              .sType(SType.RAW)
              .cType(CType.STRING)
              .dType(DataType.STRING)
              .build());
        }
      }

      csTypeMapEntry.entrySet()
          .stream()
          .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(Entry::getKey)))
          .forEach(entry -> csTypeMapSorted.put(entry.getKey().getValue(), entry.getValue()));

      sProfile.setCsTypeMap(csTypeMapSorted);

    } catch (IOException e) {
      log.catching(e);
    }
  }

  private static boolean isParsableAsLong(final String s) {
    try {
      Long.valueOf(s);
      return true;
    } catch (NumberFormatException numberFormatException) {
      return false;
    }
  }

  private static boolean isParsableAsDouble(final String s) {
    try {
      Double.valueOf(s);
      return true;
    } catch (NumberFormatException numberFormatException) {
      return false;
    }
  }
}
