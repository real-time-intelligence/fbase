package org.fbase.integration.bdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.fbase.FBase;
import org.fbase.backend.BerkleyDB;
import org.fbase.config.FBaseConfig;
import org.fbase.core.FStore;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class FBaseExistingDbStackedTest {

  private static FStore fStore;
  private static BerkleyDB berkleyDB;

  private static final String TABLE_NAME = "Query name 4";
  private static final String COLUMN_NAME = "SESSION_TYPE";
  private static final GroupFunction GROUP_FUNCTION = GroupFunction.COUNT;
  private static final long BEGIN_TIMESTAMP = 1751005932034L;
  private static final long END_TIMESTAMP = 1751005947974L;

  @BeforeAll
  static void setup() throws IOException {
    // Path to existing BerkleyDB database
    // TODO save test data
    // 27-06-2025 11:23:00
    // 27-06-2025 11:33:36
    String dbPath = "C:\\Disk\\real-time-intelligence\\releases\\real-time-intelligence-desktop-2025-debug\\database";

    // Initialize with existing database (cleanDirectory = false)
    FBaseConfig fBaseConfig = new FBaseConfig().setConfigDirectory(dbPath);
    berkleyDB = new BerkleyDB(dbPath, "ash.db", false);
    FBase fBase = new FBase(fBaseConfig, berkleyDB.getStore());
    fStore = fBase.getFStore();
  }

  @Test
  void getStackedNoFilter() throws TableNameEmptyException, SqlColMetadataException, BeginEndWrongOrderException {
    // 1. Retrieve existing table profile
    TProfile tProfile = fStore.getTProfile(TABLE_NAME);
    assertNotNull(tProfile, "Table profile should exist");
    //System.out.println(tProfile);
    System.out.println(tProfile.getCompression());

    // 2. Get the specific column profile
    List<CProfile> sessionTypeColumns = tProfile.getCProfiles().stream()
        .filter(c -> COLUMN_NAME.equals(c.getColName()))
        .collect(Collectors.toList());

    assertFalse(sessionTypeColumns.isEmpty(), "SESSION_TYPE column should exist");
    CProfile groupProfile = sessionTypeColumns.get(0);
    System.out.println(groupProfile);

    // 3. Verify column properties match your parameters
    assertEquals("VARCHAR2", groupProfile.getColDbTypeName(), "Column type should match");
    assertEquals(10, groupProfile.getColSizeDisplay(), "Column size should match");

    CSType csType = groupProfile.getCsType();
    assertNotNull(csType, "CS type should be defined");
    assertFalse(csType.isTimeStamp(), "Should not be timestamp column");
    assertEquals(SType.RAW, csType.getSType(), "S-Type should be RAW");

    // 4. Call getStacked with your specific parameters
    List<StackedColumn> result = fStore.getStacked(
        TABLE_NAME,
        groupProfile,
        GROUP_FUNCTION,
        BEGIN_TIMESTAMP,
        END_TIMESTAMP
    );

    List<List<Object>> resultRaw = fStore.getRawDataAll(
        TABLE_NAME,
        BEGIN_TIMESTAMP,
        END_TIMESTAMP
    );
    System.out.println(resultRaw);

    // 5. Print and verify results
    System.out.println("Stacked Result for " + TABLE_NAME + ":");
    System.out.println(result);

    result.forEach(column -> {
      System.out.println("Time Range: " + column.getKey() + " - " + column.getTail());
      column.getKeyCount().forEach((key, count) ->
                                       System.out.println("  " + key + ": " + count)
      );
    });

    // Basic validation
    assertNotNull(result, "Result should not be null");
    assertFalse(result.isEmpty(), "Should have at least one time bucket");

    // More specific validation based on your data
    boolean hasData = result.stream()
        .anyMatch(col -> !col.getKeyCount().isEmpty());
    assertTrue(hasData, "Should have some data in the time range");
  }

  @AfterAll
  static void cleanup() {
    if (berkleyDB != null) {
      berkleyDB.closeDatabase();
    }
  }
}