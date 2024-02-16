package org.fbase.integration.pqsql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.common.AbstractBackendSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.table.BType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBasePgSQLBackendTest extends AbstractBackendSQLTest {

  private final String dbUrl = "jdbc:postgresql://localhost:5432/postgres";
  private final String driverClassName = "org.postgresql.Driver";
  private final String tableName = "pg_backend_test";
  private final String tsName = "PG_TIMESTAMP";
  private final String select = "select * from " + tableName + " limit 1";

  private final String pg_dt_timestamp_string = "2023-09-25 13:00:00";
  private final Timestamp pg_dt_timestamp = Timestamp.valueOf(pg_dt_timestamp_string);
  private final String pg_dt_varchar = UUID.randomUUID().toString();
  private final String pg_dt_varchar_filter = UUID.randomUUID().toString();

  private SProfile sProfile;
  private TProfile tProfile;

  @BeforeAll
  public void setUp() throws SQLException, TableNameEmptyException {
    BType bType = BType.POSTGRES;
    BasicDataSource basicDataSource = getDatasource(bType, driverClassName, dbUrl, "postgres", "postgres");

    initMetaDataBackend(bType, basicDataSource);

    initDataBackendPgSql(basicDataSource);

    sProfile = getSProfileForBackend(tableName, basicDataSource, bType, select, tsName);
    tProfile = fStore.loadJdbcTableMetadata(basicDataSource.getConnection(), select, sProfile);

    log.info(tProfile);
  }

  private void initDataBackendPgSql(BasicDataSource basicDataSource) throws SQLException {
    String dropTable = """
                 DROP TABLE IF EXISTS pg_backend_test
            """;

    String createTable = """
                 CREATE TABLE IF NOT EXISTS pg_backend_test (
                       pg_timestamp timestamp,
                       pg_varchar varchar
                     )
            """;

    try (Statement statement = basicDataSource.getConnection().createStatement()) {
      statement.executeUpdate(dropTable);
      statement.executeUpdate(createTable);
    }

    String insertQuery = """
         INSERT INTO pg_backend_test (pg_timestamp, pg_varchar) 
         VALUES (?, ?)
            """;

    try (PreparedStatement ps = basicDataSource.getConnection().prepareStatement(insertQuery)) {
      ps.setTimestamp(1, pg_dt_timestamp);
      ps.setString(2, pg_dt_varchar);

      ps.executeUpdate();

      ps.setTimestamp(1, pg_dt_timestamp);
      ps.setString(2, pg_dt_varchar_filter);

      ps.executeUpdate();
    }
  }

  @Test
  public void stackedColumnTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("PG_VARCHAR");

    StackedColumn expected = StackedColumn.builder()
        .key(pg_dt_timestamp.getTime())
        .tail(pg_dt_timestamp.getTime())
        .keyCount(Map.of(pg_dt_varchar, 1, pg_dt_varchar_filter, 1))
        .build();

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actualList = fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfile, timestamps[0], timestamps[1]);

    StackedColumn actual = actualList.stream()
        .filter(f -> f.getKey() == pg_dt_timestamp.getTime())
        .findAny()
        .orElseThrow();

    assertEquals(expected.getKeyCount(), actual.getKeyCount());
  }

  @Test
  public void stackedColumnFilterTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    CProfile cProfile = getCProfileByName("PG_VARCHAR");
    CProfile cProfileFilter = getCProfileByName("PG_VARCHAR");

    StackedColumn expected = StackedColumn.builder()
        .key(pg_dt_timestamp.getTime())
        .tail(pg_dt_timestamp.getTime())
        .keyCount(Map.of(pg_dt_varchar_filter, 1))
        .build();

    long[] timestamps = getBeginEndTimestamps();
    List<StackedColumn> actualList =
        fStore.getSColumnListByCProfileFilter(tProfile.getTableName(), cProfile, cProfileFilter, pg_dt_varchar_filter, timestamps[0], timestamps[1]);

    StackedColumn actual = actualList.stream()
        .filter(f -> f.getKey() == pg_dt_timestamp.getTime())
        .findAny()
        .orElseThrow();

    assertEquals(expected.getKeyCount(), actual.getKeyCount());
  }

  @Test
  public void getLastTimestampTest() {
    long[] timestamps = getBeginEndTimestamps();

    long actual = fStore.getLastTimestamp(tProfile.getTableName(), timestamps[0], timestamps[1]);

    assertEquals(pg_dt_timestamp.getTime(), actual);
    assertEquals(pg_dt_timestamp, new Timestamp(actual));
  }

  protected long[] getBeginEndTimestamps() {
    return new long[]{pg_dt_timestamp.getTime(), pg_dt_timestamp.getTime()};
  }

  private CProfile getCProfileByName(String colName) {
    return tProfile.getCProfiles().stream()
        .filter(f -> f.getColName().equals(colName))
        .findAny().orElseThrow();
  }
}