package org.fbase.integration.graph;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractOrientDBTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * <a href="https://hub.docker.com/_/orientdb">OrientDB</a>
 *
 * docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb
 */
@Log4j2
@TestInstance(Lifecycle.PER_CLASS)
@Disabled
public class FBaseOrientDBTest extends AbstractOrientDBTest {

  private final String oUser = "select * from oUser";

  @BeforeAll
  public void initialLoading() {
    try {
      Statement stmt = dbConnection.createStatement();

      ResultSet resultSet = stmt.executeQuery(oUser);

      final AtomicInteger iRow = new AtomicInteger(0);

      ResultSetMetaData metaData = resultSet.getMetaData();
      log.info("Column count: " + metaData.getColumnCount());
      log.info("Column count: " + metaData.getColumnName(metaData.getColumnCount()));

      while (resultSet.next()) {
        int iR = iRow.addAndGet(1);

        resultSet.getInt("@version");
        resultSet.getString("@class");
        resultSet.getString("@rid");

        log.info(resultSet.getString("name"));
        log.info(iR);
      }

      resultSet.close();
      stmt.close();

    } catch (Exception e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  public void loadTest() {
    log.info("Test");
  }

}
