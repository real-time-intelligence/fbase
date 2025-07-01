package org.fbase.backend;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.model.profile.table.BType;

@Log4j2
public class SqlDB {

  @Getter
  private final BType bType;

  private final String driverClassName;
  private final String dbUrl;
  private final String username;
  private final String password;

  public SqlDB(BType bType,
               String driverClassName,
               String dbUrl,
               String username,
               String password) {
    this.bType = bType;
    this.driverClassName = driverClassName;
    this.dbUrl = dbUrl;
    this.username = username;
    this.password = password;
  }

  public BasicDataSource getDatasource() {
    BasicDataSource basicDataSource = null;
    try {
      basicDataSource = new BasicDataSource();

      basicDataSource.setDriverClassName(driverClassName);
      basicDataSource.setUrl(dbUrl);

      basicDataSource.setValidationQueryTimeout(Duration.ofSeconds(5));

      if (username != null) {
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
      }

      if (BType.CLICKHOUSE.equals(bType)) {
        basicDataSource.setValidationQuery("SELECT 1");
        basicDataSource.addConnectionProperty("compress", "0");
      }

      basicDataSource.setInitialSize(3);
      basicDataSource.setMaxTotal(7);
      basicDataSource.setMaxWait(Duration.ofMillis(TimeUnit.SECONDS.toMillis(5)));

    } catch (Exception e) {
      log.catching(e);
    }

    return basicDataSource;
  }
}
