package org.fbase.integration.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
public class FBaseMsSQLRSBackendTest extends AbstractBackendSQLTest {

  protected final String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=true;trustServerCertificate=true;";
  private final String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  private final String tableName = "MSSQL_DATA_RS";
  private final String tsName = "MSSQL_DT_TIMESTAMP";
  private final String select = "SELECT * FROM " + tableName;

  String createTableRs = """
      CREATE TABLE mssql_data_rs (
         mssql_dt_dec FLOAT,
         mssql_dt_int INT,
         mssql_dt_char NVARCHAR(32),
         mssql_dt_char_empty NVARCHAR(32),
         mssql_dt_clob NVARCHAR(MAX),
         mssql_dt_date DATE,
         mssql_dt_timestamp DATETIME2
        )
   """;

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.MSSQL;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "sa", "QOEfSsa51234!");

    // Prepare remote backend
    if (tableExists(basicDataSource.getConnection(), tableName)) {
      dropTableMSSQL(basicDataSource.getConnection(), tableName);
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
    String nvarchar = "Sample NVARCHAR";
    java.sql.Timestamp ts0 = java.sql.Timestamp.valueOf("2023-10-10 12:00:00");
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2023-10-11 12:00:00");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2023-10-12 12:00:00");
    java.sql.Timestamp ts3 = java.sql.Timestamp.valueOf("2023-10-13 12:00:00");

    String insertQuery = """
    INSERT INTO mssql_data_rs
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
        ps.setString(3, nvarchar);
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
    CProfile cProfile = getCProfileByName(tProfile,"MSSQL_DT_CHAR");
    CProfile cProfileEmpty = getCProfileByName(tProfile,"MSSQL_DT_CHAR_EMPTY");

    List<String> listActual = fStore.getDistinct(tableName, cProfile, OrderBy.DESC, 100, 0L, 4394908640000L);
    List<String> listEmptyActual = fStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, 100, 0L, 4394908640000L);

    assertEquals(List.of("Sample NVARCHAR"), listActual);
    assertEquals(Arrays.asList("Test", ""), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile numberProfile = getCProfileByName(tProfile, "MSSQL_DT_INT");
    CProfile varcharProfile = getCProfileByName(tProfile, "MSSQL_DT_CHAR");
    CProfile clobProfile = getCProfileByName(tProfile, "MSSQL_DT_CLOB");
    CProfile emptyProfile = getCProfileByName(tProfile, "MSSQL_DT_CHAR_EMPTY");

    String[] filterValues = {"Test"};
    String[] multiFilterValues = {"Test", ""};
    String[] varCharFilter = {"NVARCHAR"};
    String[] nonExistingFilter = {"NonExisting"};

    assertDistinctResults(numberProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("12345"));

    assertDistinctResults(varcharProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("Sample NVARCHAR"));

    assertDistinctResults(varcharProfile, emptyProfile, multiFilterValues, CompareFunction.EQUAL, List.of("Sample NVARCHAR"));

    assertDistinctResults(varcharProfile, varcharProfile, varCharFilter, CompareFunction.CONTAIN, List.of("Sample NVARCHAR"));

    assertDistinctResults(varcharProfile, emptyProfile, nonExistingFilter, CompareFunction.EQUAL, List.of());

    assertDistinctResults(varcharProfile, emptyProfile, null, CompareFunction.EQUAL, List.of("Sample NVARCHAR"));

    assertDistinctResults(clobProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of("B"));
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
        4394908640000L,
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
      BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, 0L, Long.MAX_VALUE, j);

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
        .filter(f -> f.getColName().equals("MSSQL_DT_CHAR_EMPTY"))
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
