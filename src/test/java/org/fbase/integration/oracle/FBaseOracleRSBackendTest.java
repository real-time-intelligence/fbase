package org.fbase.integration.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.common.AbstractBackendSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.fbase.sql.BatchResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseOracleRSBackendTest extends AbstractBackendSQLTest {

  protected final String dbUrl = "jdbc:oracle:thin:@localhost:1523:orcl";
  private final String driverClassName = "oracle.jdbc.driver.OracleDriver";
  private final String tableName = "ORACLE_DATA_RS";
  private final String tsName = "ORACLE_DT_TIMESTAMP";
  private final String select = "select * from " + tableName + " where rownum < 2";

  String createTableRs = """
           CREATE TABLE oracle_data_rs (
                  oracle_dt_dec FLOAT,
                  oracle_dt_int NUMBER,
                  oracle_dt_char VARCHAR2(32),
                  oracle_dt_char_empty VARCHAR2(32),
                  oracle_dt_clob CLOB,
                  oracle_dt_date DATE,
                  oracle_dt_timestamp TIMESTAMP(6)
                )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.ORACLE;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "system", "sys");

    // Prepare remote backend
    if (tableExists(basicDataSource.getConnection(), tableName)) {
      dropTableOracle(basicDataSource.getConnection(), tableName);
    } else {
      log.info("Skip drop operation, table not exist in DB..");
    }

    try (Statement createTableStmt = basicDataSource.getConnection().createStatement()) {
      createTableStmt.executeUpdate(createTableRs);
    }

    java.sql.Date date = java.sql.Date.valueOf("2023-10-10");
    float floatVal = 123.45f;
    String clob = "B";
    int number = 12345;
    String varchar2 = "Sample VARCHAR2";
    java.sql.Timestamp ts0 = java.sql.Timestamp.valueOf("2023-10-10 12:00:00");
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2023-10-11 12:00:00");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2023-10-12 12:00:00");
    java.sql.Timestamp ts3 = java.sql.Timestamp.valueOf("2023-10-13 12:00:00");

    String insertQuery = """
        INSERT INTO oracle_data_rs
        VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

    java.sql.Timestamp[] timestamps = new java.sql.Timestamp[]{
        ts0,
        ts1,
        ts2,
        ts3,
    };

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      for (java.sql.Timestamp timestamp : timestamps) {
        ps.setFloat(1, floatVal);
        ps.setInt(2, number);
        ps.setString(3, varchar2);
        if (ts3.equals(timestamp)) {
          ps.setString(4, "Test");
        } else {
          ps.setString(4, "");
        }
        ps.setString(5, clob);
        ps.setDate(6, date);
        ps.setTimestamp(7, timestamp);

        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(e);
      throw new RuntimeException(e);
    }

    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = fStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void getDistinctTest() throws BeginEndWrongOrderException {
    CProfile cProfile = getCProfileByName(tProfile,"ORACLE_DT_CHAR");
    CProfile cProfileEmpty = getCProfileByName(tProfile,"ORACLE_DT_CHAR_EMPTY");

    List<String> listActual = fStore.getDistinct(tableName, cProfile, OrderBy.DESC, 100, 0L, 1697357130000L);
    List<String> listEmptyActual = fStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, 100, 0L, 1697357130000L);

    assertEquals(List.of("Sample VARCHAR2"), listActual);
    assertEquals(Arrays.asList("", "Test"), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile numberProfile = getCProfileByName(tProfile, "ORACLE_DT_INT");
    CProfile varchar2Profile = getCProfileByName(tProfile, "ORACLE_DT_CHAR");
    CProfile clobProfile = getCProfileByName(tProfile, "ORACLE_DT_CLOB");
    CProfile emptyProfile = getCProfileByName(tProfile, "ORACLE_DT_CHAR_EMPTY");

    String[] filterValues = {"Test"};
    String[] multiFilterValues = {"Test", ""};
    String[] varCharFilter = {"VAR"};
    String[] nonExistingFilter = {"NonExisting"};

    assertDistinctResults(numberProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("12345"));

    assertDistinctResults(varchar2Profile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));

    assertDistinctResults(varchar2Profile, emptyProfile, multiFilterValues, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));

    assertDistinctResults(varchar2Profile, varchar2Profile, varCharFilter, CompareFunction.CONTAIN, List.of("Sample VARCHAR2"));

    assertDistinctResults(varchar2Profile, emptyProfile, nonExistingFilter, CompareFunction.EQUAL, List.of());

    assertDistinctResults(varchar2Profile, emptyProfile, null, CompareFunction.EQUAL, List.of("Sample VARCHAR2"));

    assertThrows(RuntimeException.class, () -> assertDistinctResults(clobProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("B")));
  }

  private void assertDistinctResults(CProfile targetProfile, CProfile filterProfile,
                                     String[] filterValues, CompareFunction compareFunction,
                                     List<String> expectedResults) throws BeginEndWrongOrderException {
    List<String> actualResults = fStore.getDistinct(
        tableName,
        targetProfile,
        OrderBy.ASC,
        100,
        0L,
        1697357130000L,
        filterProfile,
        filterValues,
        compareFunction
    );
    assertEquals(expectedResults, actualResults);
  }

  @Test
  public void batchResultTest() {
    int fetchSize = 5;

    for (int j = 0; j < fetchSize; j++) {
      BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, 0L, 1697357130000L, j);

      while (batchResultSet.next()) {
        List<List<Object>> var = batchResultSet.getObject();

        if (j == 0) {
          assertEquals(1, var.size());
        } else if (j == 1) {
          assertEquals(1, var.size());
        } else if (j == 2) {
          assertEquals(2, var.size());
        } else if (j == 3) {
          assertEquals(3, var.size());
        } else if (j == 4) {
          assertEquals(4, var.size());
        }

        break;
      }
    }
  }

  @Test
  public void batchResultSingleTest() {
    BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, 0L, 4394908640000L, 2);

    int count = 0;
    while (batchResultSet.next()) {
      log.info(batchResultSet.getObject());

      count++;
    }

    assertEquals(2, count);
  }

  @Test
  public void batchResultSingleEmptyDataTest() {
    BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, 4394908640000L, Long.MAX_VALUE, 10);

    int count = 0;
    while (batchResultSet.next()) {
      log.info(batchResultSet.getObject());

      count++;
    }

    assertEquals(0, count);
  }

  @Test
  public void bugEmptyValueTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = tProfile.getCProfiles()
        .stream()
        .filter(f -> f.getColName().equals("ORACLE_DT_CHAR_EMPTY"))
        .findAny()
        .orElseThrow();

    List<StackedColumn> stackedColumns =
        fStore.getStacked(tableName, cProfile, GroupFunction.COUNT,
                          null, null, null,
                          0, 4394908640000L);

    assertEquals(3, stackedColumns.get(0).getKeyCount().get(""));
    assertEquals(1, stackedColumns.get(0).getKeyCount().get("Test"));
  }
}
