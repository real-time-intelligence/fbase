package org.fbase.integration.pqsql;

import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractOracleTest;
import org.fbase.common.AbstractPostgreSQLTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.sql.SQLException;
import java.util.List;

@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBasePostgreSQLTest extends AbstractPostgreSQLTest {

  private final String selectAsh = "SELECT current_timestamp as SAMPLE_TIME, "
          + "datid, datname, "
          + "pid AS SESSION_ID, pid AS SESSION_SERIAL, usesysid AS USER_ID, "
          + "coalesce(usename, 'unknown') as usename, "
          + "concat(application_name,'::', backend_type, '::', coalesce(client_hostname, client_addr::text, 'localhost')) AS PROGRAM, "
          + "wait_event_type AS WAIT_CLASS, wait_event AS EVENT, query, substring(md5(query) from 0 for 15) AS SQL_ID, "
          + "left(query, strpos(query, ' '))  AS SQL_OPNAME, "
          + "coalesce(query_start, xact_start, backend_start) as query_start, "
          + "1000 * EXTRACT(EPOCH FROM (clock_timestamp()-coalesce(query_start, xact_start, backend_start))) as duration "
          + "from pg_stat_activity "
          + "where state='active'";
  private final String selectRandom = "SELECT current_timestamp as dt " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_histogram " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_enum " +
          "        ,MIN(CASE WHEN rn = 7 THEN floor(random()*(10-1+1))+1 END) as value_raw " +
          "  FROM generate_series(1,50) id " +
          "  ,LATERAL( SELECT nr, ROW_NUMBER() OVER (ORDER BY id * random()) " +
          "             FROM generate_series(1,900) nr " +
          "          ) sub(nr, rn) " +
          "   GROUP BY id";

  @BeforeAll
  public void initialLoading() {
    try {
      loadData(fStore, dbConnection, selectRandom, getSProfileForRandom(), log,20000, 20000);
      loadData(fStore, dbConnection, selectAsh, getSProfileForAsh(selectAsh), log,20000, 20000);
    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void selectRandomTest()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {
    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectRandom, getSProfileForRandom());
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileHistogram = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_HISTOGRAM")).findAny().get();
    CProfile cProfileEnum = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_ENUM")).findAny().get();
    CProfile cProfileRaw = cProfiles.stream().filter(f -> f.getColName().equals("VALUE_RAW")).findAny().get();

    List<StackedColumn> stackedColumnsHistogram =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileHistogram, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsEnum =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileEnum, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsRaw =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileRaw, 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsHistogram);
    System.out.println(stackedColumnsEnum);
    System.out.println(stackedColumnsRaw);
  }

  @Test
  public void selectAshTest()
      throws SQLException, BeginEndWrongOrderException, SqlColMetadataException {

    SProfile sProfile = getSProfileForAsh(selectAsh);

    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectAsh, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileSampleTime = cProfiles.stream().filter(f -> f.getColName().equals("USER_ID")).findAny().get();
    CProfile cProfileSqlId = cProfiles.stream().filter(f -> f.getColName().equals("SQL_ID")).findAny().get();
    CProfile cProfileEvent = cProfiles.stream().filter(f -> f.getColName().equals("EVENT")).findAny().get();

    List<StackedColumn> stackedColumnsBySampleTime =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSampleTime, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsBySqlId =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSqlId, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByEvent =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileEvent, 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsBySampleTime);
    System.out.println(stackedColumnsBySqlId);
    System.out.println(stackedColumnsByEvent);
  }

}
