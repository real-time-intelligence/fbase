package org.fbase.common.mode;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.fbase.core.FStore;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.sql.BatchResultSet;

public interface CSVMode {

  default SProfile getSProfileCsv(
      String tableName,
      String fileName,
      String csvSplitBy,
      TType tableType,
      IType indexType,
      BType backendType,
      boolean compression
  ) {
    SProfile sProfile = SProfile.builder()
        .tableName(tableName)
        .tableType(tableType)
        .indexType(indexType)
        .backendType(backendType)
        .compression(compression)
        .csTypeMap(new LinkedHashMap<>()).build();

    MetadataHandler.loadMetadataFromCsv(fileName, csvSplitBy, sProfile);

    return sProfile;
  }

  default void assertDataCsvBatchTest(FStore fStore, SProfile sProfile, String fileNameCsv, boolean compression, int fetchSize, boolean eventFetchSize)
      throws IOException, TableNameEmptyException {
    String csvSplitBy = ",";

    String tableName = sProfile.getTableName();

    String expected = readFile(fileNameCsv, Charset.defaultCharset());

    List<List<Object>> rawDataAll = new ArrayList<>();

    BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, fetchSize);

    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      if (eventFetchSize) {
        assertEquals(fetchSize, var.size());
      }
      rawDataAll.addAll(var);
    }

    String actual = toCsvFile(rawDataAll, fStore.getTProfile(sProfile.getTableName()), csvSplitBy);

    assertEquals(expected, actual);
  }

  default String toCsvFile(List<List<Object>> data, TProfile tProfile, String csvSplitBy) {
    StringBuilder output = new StringBuilder();

    // headers
    AtomicInteger headerCounter = new AtomicInteger(0);
    List<CProfile> cProfiles = tProfile.getCProfiles();
    cProfiles.stream()
        .sorted(Comparator.comparing(CProfile::getColId))
        .forEach(cProfile -> {
          headerCounter.getAndAdd(1);
          output.append(cProfile.getColName()).append(headerCounter.get() < cProfiles.size() ? csvSplitBy : "");
        });

    output.append("\n");

    // data
    AtomicInteger counter = new AtomicInteger(0);
    for (List<Object> rowData : data) {
      counter.getAndAdd(1);
      for (int i = 0; i < rowData.size(); i++) {
        output.append(rowData.get(i).toString());
        if (i < rowData.size() - 1) {
          output.append(csvSplitBy);
        }
      }

      if (counter.get() < data.size()) {
        output.append("\n");
      }
    }

    return output.toString();
  }

  static String readFile(String path, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding).replace("\r", "");
  }
}
