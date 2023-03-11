package org.fbase.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.sql.BatchResultSet;

/**
 * Main API for data management
 */
public interface FStore {

  /**
   * Get Table profile
   * @param tableName - Table name
   * @return TProfile - Table profile
   */
  TProfile getTProfile(String tableName) throws TableNameEmptyException;

  /**
   * Load metadata from connection using query
   * @param connection - JDBC connection
   * @param query - Query text
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadJdbcTableMetadata(Connection connection, String query, SProfile sProfile)
      throws SQLException, TableNameEmptyException;

  /**
   * Load metadata from csv file
   * @param fileName - CSV file name
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadCsvTableMetadata(String fileName, String csvSplitBy, SProfile sProfile)
      throws TableNameEmptyException;

  /**
   * Save data in table using intermediate structure
   * @param tableName - Table name
   * @param data - Data to store
   */
  void putDataDirect(String tableName, List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table directly from JDBC ResultSet
   * @param tableName - Table name
   * @param resultSet - ResultSet to get data from sql source
   * @return long - timestamp value of column for last row
   */
  long putDataJdbc(String tableName, ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table in batch mode from JDBC ResultSet
   * @param tableName - Table name
   * @param resultSet - ResultSet to get data from sql source
   * @param fBaseBatchSize - batch size
   */
  void putDataJdbcBatch(String tableName, ResultSet resultSet, Integer fBaseBatchSize) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table for csv file in batch mode
   * @param tableName - Table name
   * @param fileName - CSV file name
   */
  void putDataCsvBatch(String tableName, String fileName, String csvSplitBy, Integer fBaseBatchSize) throws SqlColMetadataException;

  /**
   * Get list stacked data by column
   * @param tableName - Table name
   * @param cProfile - Column profile
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<StackedColumn>} - list data in StackedColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<StackedColumn> getSColumnListByCProfile(String tableName, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;

  /**
   * Get list of gantt data
   * @param tableName - Table name
   * @param firstLevelGroupBy - Column profile for first level
   * @param secondLevelGroupBy - Column profile for second level
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumn> getGColumnListTwoLevelGroupBy(String tableName,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get raw data by column
   * @param tableName - Table name
   * @param cProfile - Column profile to get data from
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<Object>} - list raw data by column
   */
  List<List<Object>> getRawDataByColumn(String tableName, CProfile cProfile, long begin, long end);

  /**
   * Get list of raw data
   * @param tableName - Table name
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<List<Object>>} - list raw data
   */
  List<List<Object>> getRawDataAll(String tableName, long begin, long end);

  /**
   * Get list of raw data using BatchResultSet
   * @param tableName - Table name
   * @param fetchSize - the number of rows to fetch
   * @return {@literal BatchResultSet} - batch result set
   */
  BatchResultSet getBatchResultSet(String tableName, int fetchSize);

  /**
   * Get list of raw data for time-series table using BatchResultSet
   * @param tableName - Table name
   * @param begin - start of range
   * @param end - end of range
   * @param fetchSize - the number of rows to fetch
   * @return {@literal BatchResultSet} - batch result set
   */
  BatchResultSet getBatchResultSet(String tableName, long begin, long end, int fetchSize);

  /**
   * Get last timestamp
   * @param tableName - Table name
   * @param begin - start of range
   * @param end - end of range
   * @return
   */
  long getLastTimestamp(String tableName, long begin, long end);

  /**
   * Sync data to persistent storage
   */
  void syncBackendDb();

  /**
   * Close persistent storage
   */
  void closeBackendDb();
}
