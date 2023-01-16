package org.fbase.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.function.GBFunction;
import org.fbase.model.output.GroupByColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;

/**
 * Main API for data management
 */
public interface FStore {

  /**
   * Get table profile
   * @param query - Query text
   * @return TProfile - Table profile
   */
  TProfile getTProfile(String query);

  /**
   * Load metadata from connection using query
   * @param connection - JDBC connection
   * @param query - Query text
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadJdbcTableMetadata(Connection connection, String query, SProfile sProfile) throws SQLException;

  /**
   * Load metadata from csv file
   * @param fileName - CSV file name
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadCsvTableMetadata(String fileName, String csvSplitBy, SProfile sProfile);

  /**
   * Get list of column metadata for table
   * @param tProfile - Table profile
   * @return {@literal List<CProfile>} - list of column metadata
   */
  List<CProfile> getCProfileList(TProfile tProfile);

  /**
   * Save data for table profile using intermediate structure
   * @param tProfile - Table profile
   * @param data - Data to store
   */
  void putDataDirect(TProfile tProfile, List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data for table profile directly from JDBC ResultSet
   * @param tProfile - Table profile
   * @param resultSet - ResultSet to get data from sql source
   * @return long - timestamp value of column for last row
   */
  long putDataJdbc(TProfile tProfile, ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data for table profile in batch mode from JDBC ResultSet
   * @param tProfile - Table profile
   * @param resultSet - ResultSet to get data from sql source
   * @param fBaseBatchSize - batch size
   */
  void putDataJdbcBatch(TProfile tProfile, ResultSet resultSet, Integer fBaseBatchSize) throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data for csv file in batch mode
   * @param tProfile - Table profile
   * @param fileName - CSV file name
   */
  void putDataCsvBatch(TProfile tProfile, String fileName, String csvSplitBy, Integer fBaseBatchSize) throws SqlColMetadataException;

  /**
   * Get list stacked data by column
   * @param tProfile - Table profile
   * @param cProfile - Column profile
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<StackedColumn>} - list data in StackedColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<StackedColumn> getSColumnListByCProfile(TProfile tProfile, CProfile cProfile, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;

  /**
   * Get list of gantt data
   * @param tProfile - Table profile
   * @param firstLevelGroupBy - Column profile for first level
   * @param secondLevelGroupBy - Column profile for second level
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumn> getGColumnListTwoLevelGroupBy(TProfile tProfile,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;


  /**
   * Get result of group by function
   * @param tProfile - Table profile
   * @param firstLevelGroupBy - Column profile for first level
   * @param secondLevelGroupBy - Column profile for second level
   * @param gbFunction - function (COUNT, SUM etc)
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GroupByColumn> getGBColumnListTwoLevelGroupBy(TProfile tProfile,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, GBFunction gbFunction, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get raw data by column
   * @param tProfile - Table profile
   * @param cProfile - Column profile to get data from
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<Object>} - list raw data by column
   */
  List<List<Object>> getRawDataByColumn(TProfile tProfile, CProfile cProfile, long begin, long end);

  /**
   * Get list of raw data
   * @param tProfile - Table profile
   * @param begin - start of range
   * @param end - end of range
   * @return {@literal List<List<Object>>} - list raw data
   */
  List<List<Object>> getRawDataAll(TProfile tProfile, long begin, long end);

  /**
   * Get list of raw data
   * @param tProfile - Table profile
   * @return {@literal List<List<Object>>} - list raw data
   */
  List<List<Object>> getRawDataAll(TProfile tProfile);

  /**
   * Get last timestamp
   * @param tProfile - Table profile
   * @param begin - start of range
   * @param end - end of range
   * @return
   */
  long getLastTimestamp(TProfile tProfile, long begin, long end);

  /**
   * Sync data to persistent storage
   */
  void syncBackendDb();

  /**
   * Close persistent storage
   */
  void closeBackendDb();
}
