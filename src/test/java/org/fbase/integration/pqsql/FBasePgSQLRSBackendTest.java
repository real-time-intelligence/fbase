package org.fbase.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
public class FBasePgSQLRSBackendTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:postgresql://localhost:5432/postgres";
  private final String driverClassName = "org.postgresql.Driver";
  private final String tableName = "pg_data_rs";
  private final String tsName = "PG_DT_TIMESTAMP";
  private final String select = "select * from " + tableName + " limit 1";

  String createTableRs = """
           CREATE TABLE pg_data_rs (
                  pg_dt_dec float8,
                  pg_dt_int int8,
                  pg_dt_byte int8,
                  pg_dt_bool bool,
                  pg_dt_char varchar,
                  pg_dt_char_empty varchar,
                  pg_dt_clob text,
                  pg_dt_date date,
                  pg_dt_timestamp timestamp
                )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  // TODO investigate by days
  LocalDateTime pg_dt_timestamp0 = LocalDateTime.of(2023, 1, 10, 16, 5, 20);
  LocalDateTime pg_dt_timestamp1 = LocalDateTime.of(2023, 2, 12, 16, 5, 20);
  LocalDateTime pg_dt_timestamp2 = LocalDateTime.of(2023, 3, 14, 16, 5, 20);
  LocalDateTime pg_dt_timestamp3 = LocalDateTime.of(2023, 4, 16, 16, 5, 20);

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.POSTGRES;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "postgres", "postgres");

    // Prepare remote backend
    dropTable(basicDataSource.getConnection(), tableName);

    try (Statement createTableStmt = basicDataSource.getConnection().createStatement()) {
      createTableStmt.executeUpdate(createTableRs);
    }

    BigDecimal pg_dt_dec = new BigDecimal("1234.56");
    int pg_dt_int = 6789;
    byte pg_dt_byte = 12;
    boolean pg_dt_bool = true;
    String pg_dt_char = "A";
    String pg_dt_clob = "Lorem ipsum dolor sit amet";
    LocalDate pg_dt_date = LocalDate.of(2023, 10, 13);

    String insertQuery = """
        INSERT INTO pg_data_rs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

    LocalDateTime[] timestamps = new LocalDateTime[]{
        pg_dt_timestamp0,
        pg_dt_timestamp1,
        pg_dt_timestamp2,
        pg_dt_timestamp3
    };

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      for (LocalDateTime timestamp : timestamps) {
        ps.setBigDecimal(1, pg_dt_dec);
        ps.setInt(2, pg_dt_int);
        ps.setByte(3, pg_dt_byte);
        ps.setBoolean(4, pg_dt_bool);
        ps.setString(5, pg_dt_char);
        ps.setString(6, "");
        ps.setString(7, pg_dt_clob);
        ps.setDate(8, java.sql.Date.valueOf(pg_dt_date));
        ps.setTimestamp(9, java.sql.Timestamp.valueOf(timestamp));

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
    CProfile cProfile = getCProfileByName(tProfile,"PG_DT_CHAR");
    CProfile cProfileEmpty = getCProfileByName(tProfile,"PG_DT_CHAR_EMPTY");

    List<String> listActual = fStore.getDistinct(tableName, cProfile, OrderBy.DESC, 100, 0L, 1697357130000L);
    List<String> listEmptyActual = fStore.getDistinct(tableName, cProfileEmpty, OrderBy.DESC, 100, 0L, 1697357130000L);

    assertEquals(List.of("A"), listActual);
    assertEquals(List.of(""), listEmptyActual);
  }

  @Test
  public void getDistinctTestWithFilter() throws BeginEndWrongOrderException {
    CProfile intProfile = getCProfileByName(tProfile, "PG_DT_INT");
    CProfile charProfile = getCProfileByName(tProfile, "PG_DT_CHAR");
    CProfile clobProfile = getCProfileByName(tProfile, "PG_DT_CLOB");
    CProfile emptyProfile = getCProfileByName(tProfile, "PG_DT_CHAR_EMPTY");

    String[] filterValues = {"A"};
    String[] multiFilterValues = {"A", ""};
    String[] charFilter = {"A"};
    String[] nonExistingFilter = {"NonExisting"};
    String[] clobFilter = {"Lorem ipsum dolor sit amet"};

    assertDistinctResults(intProfile, charProfile, filterValues, CompareFunction.EQUAL, List.of("6789"));

    assertDistinctResults(charProfile, emptyProfile, filterValues, CompareFunction.EQUAL, List.of());

    assertDistinctResults(charProfile, emptyProfile, multiFilterValues, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(charProfile, charProfile, charFilter, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(charProfile, emptyProfile, nonExistingFilter, CompareFunction.EQUAL, List.of());

    assertDistinctResults(charProfile, emptyProfile, null, CompareFunction.EQUAL, List.of("A"));

    assertDistinctResults(clobProfile, clobProfile, clobFilter, CompareFunction.EQUAL, List.of("Lorem ipsum dolor sit amet"));
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
        .filter(f -> f.getColName().equals("PG_DT_CHAR_EMPTY"))
        .findAny()
        .orElseThrow();

    List<StackedColumn> stackedColumns =
        fStore.getStacked(tableName, cProfile, GroupFunction.COUNT,
                          null, null, null,
                          0, 4394908640000L);

    assertEquals(4, stackedColumns.get(0).getKeyCount().get(""));
  }
}
