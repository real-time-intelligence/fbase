package org.fbase.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.fbase.exception.TableNameEmptyException;
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
                  pg_dt_clob varchar,
                  pg_dt_date date,
                  pg_dt_timestamp timestamp
                )
      """;

  private SProfile sProfile;
  private TProfile tProfile;

  LocalDateTime pg_dt_timestamp = LocalDateTime.of(2023, 10, 13, 16, 5, 20);

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.CLICKHOUSE;
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

    LocalDateTime[] timestamps = new LocalDateTime[]{
        pg_dt_timestamp,
        pg_dt_timestamp.plusDays(1),
        pg_dt_timestamp.plusDays(2),
        pg_dt_timestamp.plusDays(3)
    };

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      for (LocalDateTime timestamp : timestamps) {
        ps.setBigDecimal(1, pg_dt_dec);
        ps.setInt(2, pg_dt_int);
        ps.setByte(3, pg_dt_byte);
        ps.setBoolean(4, pg_dt_bool);
        ps.setString(5, pg_dt_char);
        ps.setString(6, pg_dt_clob);
        ps.setDate(7, java.sql.Date.valueOf(pg_dt_date));
        ps.setTimestamp(8, java.sql.Timestamp.valueOf(timestamp));

        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.info(e);
    }

    // Init FBase backend
    initMetaDataBackend(bType, basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = fStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  @Test
  public void batchResultTest() {
    int fetchSize = 4;

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
            assertEquals(3, var.size());
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
}
