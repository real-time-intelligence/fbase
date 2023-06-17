package org.fbase.integration.mssql;

import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractMicrosoftSQLTest;
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
public class FBaseMicrosoftSQLTest extends AbstractMicrosoftSQLTest {

  private final String selectAshLike = "SELECT getdate() as dt, s.session_id, s.login_time, s.host_name, s.program_name,\n" +
          "s.login_name, s.nt_user_name, s.is_user_process,\n" +
          "s.database_id, DB_NAME(s.database_id) AS [database], \n" +
          "s.status,\n" +
          "s.reads, s.writes, s.logical_reads, s.row_count,\n" +
          "c.session_id, c.net_transport, c.protocol_type, \n" +
          "c.client_net_address, c.client_tcp_port, \n" +
          "c.num_writes AS DataPacketWrites \n" +
          "FROM sys.dm_exec_sessions s\n" +
          "INNER JOIN sys.dm_exec_connections c\n" +
          "ON s.session_id = c.session_id \n" +
          "INNER JOIN sys.dm_exec_requests r \n" +
          "ON s.session_id = r.session_id";
  private final String selectRandom = "SELECT GETDATE() as dt\n" +
          "        ,MIN(floor(rand()*(10-1+1))+1) as value_histogram\n" +
          "        ,MIN(floor(rand()*(10-1+1))+2) as value_enum\n" +
          "        ,MIN(floor(rand()*(10-1+1))+3) as value_raw \n" +
          "  FROM \n" +
          "  (SELECT id, nr, ROW_NUMBER() OVER (ORDER BY id * rand()) as rn\n" +
          "             FROM \n" +
          "             (SELECT * FROM \n" +
          "             (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id FROM sys.objects a CROSS JOIN sys.objects b) t1\n" +
          "             CROSS JOIN \n" +
          "             (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS nr FROM sys.objects a CROSS JOIN sys.objects b) t2) t3) sub\n" +
          "   GROUP BY id";

  @BeforeAll
  public void initialLoading() {
    try {
      loadData(fStore, dbConnection, selectRandom, getSProfileForRandom(), log,20000, 20000);
      loadData(fStore, dbConnection, selectAshLike, getSProfileForAsh(selectAshLike), log,20000, 20000);
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

    SProfile sProfile = getSProfileForAsh(selectAshLike);

    TProfile tProfile;
    try {
      tProfile = fStore.loadJdbcTableMetadata(dbConnection, selectAshLike, sProfile);
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    List<CProfile> cProfiles = tProfile.getCProfiles();

    CProfile cProfileLoginName = cProfiles.stream().filter(f -> f.getColName().equals("LOGIN_NAME")).findAny().get();
    CProfile cProfileSessId = cProfiles.stream().filter(f -> f.getColName().equals("SESSION_ID")).findAny().get();
    CProfile cProfileProgramName = cProfiles.stream().filter(f -> f.getColName().equals("PROGRAM_NAME")).findAny().get();
    CProfile cProfileIsUserProcess = cProfiles.stream().filter(f -> f.getColName().equals("IS_USER_PROCESS")).findAny().get();

    List<StackedColumn> stackedColumnsBySampleTime =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileLoginName, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsBySqlId =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileSessId, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByEvent =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileProgramName, 0, Long.MAX_VALUE);
    List<StackedColumn> stackedColumnsByIsUserProcess =
        fStore.getSColumnListByCProfile(tProfile.getTableName(), cProfileIsUserProcess, 0, Long.MAX_VALUE);

    List<List<Object>> rawData = fStore.getRawDataAll(tProfile.getTableName(), 0, Long.MAX_VALUE);

    System.out.println(stackedColumnsBySampleTime);
    System.out.println(stackedColumnsBySqlId);
    System.out.println(stackedColumnsByEvent);
    System.out.println(stackedColumnsByIsUserProcess);

    System.out.println(rawData);
  }

}
