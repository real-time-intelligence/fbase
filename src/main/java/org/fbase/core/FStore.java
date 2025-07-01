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
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.OrderBy;
import org.fbase.model.output.BlockKeyTail;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.GanttColumnSum;
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
   *
   * @param tableName - Table name
   * @return TProfile - Table profile
   */
  TProfile getTProfile(String tableName) throws TableNameEmptyException;

  /**
   * Load metadata from storage profile directly
   *
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadDirectTableMetadata(SProfile sProfile) throws TableNameEmptyException;

  /**
   * Load metadata from connection using query
   *
   * @param connection - JDBC connection
   * @param query      - Query text
   * @param sProfile   - Storage profile
   * @return TProfile  - Table profile
   */
  TProfile loadJdbcTableMetadata(Connection connection,
                                 String query,
                                 SProfile sProfile)
      throws SQLException, TableNameEmptyException;


  /**
   * Load metadata from connection using schema and table name (Oracle specific)
   *
   * @param connection    - JDBC connection
   * @param sqlSchemaName - Schema name
   * @param sqlTableName  - Table name
   * @param sProfile      - Storage profile
   * @return TProfile     - Table profile
   */
  TProfile loadJdbcTableMetadata(Connection connection,
                                 String sqlSchemaName,
                                 String sqlTableName,
                                 SProfile sProfile)
      throws SQLException, TableNameEmptyException;

  /**
   * Load metadata from csv file
   *
   * @param fileName - CSV file name
   * @param sProfile - Storage profile
   * @return TProfile - Table profile
   */
  TProfile loadCsvTableMetadata(String fileName,
                                String csvSplitBy,
                                SProfile sProfile)
      throws TableNameEmptyException;

  /**
   * Update Table profile with new timestamp column
   *
   * @param tableName           - Table name
   * @param timestampColumnName - Timestamp column name
   */
  void setTimestampColumn(String tableName,
                          String timestampColumnName)
      throws TableNameEmptyException;

  /**
   * Save data in table using intermediate Java table structure
   *
   * @param tableName - Table name
   * @param data      - Intermediate table structure as Java objects to store data
   */
  void putDataDirect(String tableName,
                     List<List<Object>> data)
      throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table directly from JDBC ResultSet
   *
   * @param tableName - Table name
   * @param resultSet - ResultSet to get data from sql source
   * @return long - timestamp value of column for last row
   */
  long putDataJdbc(String tableName,
                   ResultSet resultSet)
      throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table in batch mode from JDBC ResultSet
   *
   * @param tableName      - Table name
   * @param resultSet      - ResultSet to get data from sql source
   * @param fBaseBatchSize - batch size
   */
  void putDataJdbcBatch(String tableName,
                        ResultSet resultSet,
                        Integer fBaseBatchSize)
      throws SqlColMetadataException, EnumByteExceedException;

  /**
   * Save data in table for csv file in batch mode
   *
   * @param tableName - Table name
   * @param fileName  - CSV file name
   */
  void putDataCsvBatch(String tableName,
                       String fileName,
                       String csvSplitBy,
                       Integer fBaseBatchSize)
      throws SqlColMetadataException;


  /**
   * Get list of blocks with key and tail in selected range
   *
   * @param tableName     - Table name
   * @param begin         - start of range
   * @param end           - end of range
   * @return {@literal List<BlockKeyTail>} - list data for key and tail for blocks
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                         long begin,
                                         long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;

  /**
   * Get list stacked data by column
   *
   * @param tableName     - Table name
   * @param cProfile      - Column profile
   * @param groupFunction - COUNT, SUM or AVG
   * @param begin         - start of range
   * @param end           - end of range
   * @return {@literal List<StackedColumn>} - list data in StackedColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<StackedColumn> getStacked(String tableName,
                                 CProfile cProfile,
                                 GroupFunction groupFunction,
                                 long begin,
                                 long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;

  /**
   * Get list stacked data by column
   *
   * @param tableName       - Table name
   * @param cProfile        - Column profile
   * @param groupFunction   - COUNT, SUM or AVG
   * @param cProfileFilter  - Column profile filter
   * @param filterData      - Array of filter values for column profile filter
   * @param compareFunction - Compares filterData values using EQUAL or CONTAINS functions
   * @param begin           - start of range
   * @param end             - end of range
   * @return {@literal List<StackedColumn>} - list data in StackedColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<StackedColumn> getStacked(String tableName,
                                 CProfile cProfile,
                                 GroupFunction groupFunction,
                                 CProfile cProfileFilter,
                                 String[] filterData,
                                 CompareFunction compareFunction,
                                 long begin,
                                 long end)
      throws SqlColMetadataException, BeginEndWrongOrderException;

  /**
   * Get list of gantt data in single threading
   *
   * @param tableName   - Table name
   * @param firstGrpBy  - Column profile for first level
   * @param secondGrpBy - Column profile for second level
   * @param begin       - start of range
   * @param end         - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumnCount> getGantt(String tableName,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  long begin,
                                  long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get list of gantt data using multithreading
   *
   * @param tableName   - Table name
   * @param firstGrpBy  - Column profile for first level
   * @param secondGrpBy - Column profile for second level
   * @param batchSize   - batch size of multithreading
   * @param begin       - start of range
   * @param end         - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumnCount> getGantt(String tableName,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  int batchSize,
                                  long begin,
                                  long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get list of gantt data
   *
   * @param tableName       - Table name
   * @param firstGrpBy      - Column profile for first level
   * @param secondGrpBy     - Column profile for second level
   * @param cProfileFilter  - Column profile filter
   * @param filterData      - Array of filter values for column profile filter
   * @param compareFunction - Compares filterData values using EQUAL or CONTAINS functions
   * @param begin           - start of range
   * @param end             - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumnCount> getGantt(String tableName,
                                  CProfile firstGrpBy,
                                  CProfile secondGrpBy,
                                  CProfile cProfileFilter,
                                  String[] filterData,
                                  CompareFunction compareFunction,
                                  long begin,
                                  long end)
  throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get list of one-level gantt data using sum function
   *
   * @param tableName   - Table name
   * @param firstGrpBy  - Column profile for first level
   * @param secondGrpBy - Column profile for second level (ONLY number data type supported)
   * @param begin       - start of range
   * @param end         - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumnSum> getGanttSum(String tableName,
                                   CProfile firstGrpBy,
                                   CProfile secondGrpBy,
                                   long begin,
                                   long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get list of one-level gantt data using sum function with filter
   *
   * @param tableName       - Table name
   * @param firstGrpBy      - Column profile for first level
   * @param secondGrpBy     - Column profile for second level (ONLY number data type supported)
   * @param cProfileFilter  - Column profile filter
   * @param filterData      - Array of filter values for column profile filter
   * @param compareFunction - Compares filterData values using EQUAL or CONTAINS functions
   * @param begin           - start of range
   * @param end             - end of range
   * @return {@literal List<GanttColumn>} - list data in GanttColumn
   * @throws SqlColMetadataException
   * @throws BeginEndWrongOrderException
   */
  List<GanttColumnSum> getGanttSum(String tableName,
                                   CProfile firstGrpBy,
                                   CProfile secondGrpBy,
                                   CProfile cProfileFilter,
                                   String[] filterData,
                                   CompareFunction compareFunction,
                                   long begin,
                                   long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException;

  /**
   * Get list of distinct data by column
   *
   * @param tableName   - Table name
   * @param cProfile    - Column profile for first level
   * @param orderBy     - ASC or DESC order
   * @param limit       - limit data in list
   * @param begin       - start of range
   * @param end         - end of range
   * @return {@literal List<String>} - list distinct data
   * @throws BeginEndWrongOrderException
   */
  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end)
      throws BeginEndWrongOrderException;

  /**
   * Get list of distinct data by column
   *
   * @param tableName   - Table name
   * @param cProfile    - Column profile for first level
   * @param orderBy     - ASC or DESC order
   * @param limit       - limit data in list
   * @param begin       - start of range
   * @param end         - end of range
   * @param cProfileFilter  - Column profile filter
   * @param filterData      - Array of filter values for column profile filter
   * @param compareFunction - Compares filterData values using EQUAL or CONTAINS functions
   * @return {@literal List<String>} - list distinct data
   * @throws BeginEndWrongOrderException
   */
  List<String> getDistinct(String tableName,
                           CProfile cProfile,
                           OrderBy orderBy,
                           int limit,
                           long begin,
                           long end,
                           CProfile cProfileFilter,
                           String[] filterData,
                           CompareFunction compareFunction)
      throws BeginEndWrongOrderException;

  /**
   * Get list of raw data
   *
   * @param tableName - Table name
   * @param begin     - start of range
   * @param end       - end of range
   * @return {@literal List<List<Object>>} - list raw data
   */
  List<List<Object>> getRawDataAll(String tableName,
                                   long begin,
                                   long end);

  /**
   * Get list of raw data with filter by column
   *
   * @param tableName      - Table name
   * @param cProfileFilter - Column profile filter
   * @param filter         - Filter value for column profile filter
   * @param begin          - start of range
   * @param end            - end of range
   * @return {@literal List<List<Object>>} - list raw data
   */
  List<List<Object>> getRawDataAll(String tableName,
                                   CProfile cProfileFilter,
                                   String filter,
                                   long begin,
                                   long end);

  /**
   * Get raw data by column
   *
   * @param tableName - Table name
   * @param cProfile  - Column profile to get data from
   * @param begin     - start of range
   * @param end       - end of range
   * @return {@literal List<Object>} - list raw data by column
   */
  List<List<Object>> getRawDataByColumn(String tableName,
                                        CProfile cProfile,
                                        long begin,
                                        long end);

  /**
   * Get list of raw data for regular table using BatchResultSet
   *
   * @param tableName - Table name
   * @param fetchSize - the number of rows to fetch
   * @return {@literal BatchResultSet} - batch result set
   */
  BatchResultSet getBatchResultSet(String tableName,
                                   int fetchSize);

  /**
   * Get list of raw data for time-series table using BatchResultSet
   *
   * @param tableName - Table name
   * @param begin     - start of range
   * @param end       - end of range
   * @param fetchSize - the number of rows to fetch
   * @return {@literal BatchResultSet} - batch result set
   */
  BatchResultSet getBatchResultSet(String tableName,
                                   long begin,
                                   long end,
                                   int fetchSize);

  /**
   * Get first timestamp value
   *
   * @param tableName - Table name
   * @param begin     - start of range
   * @param end       - end of range
   * @return
   */
  long getFirst(String tableName,
               long begin,
               long end);

  /**
   * Get last timestamp value
   *
   * @param tableName - Table name
   * @param begin     - start of range
   * @param end       - end of range
   * @return
   */
  long getLast(String tableName,
               long begin,
               long end);

  /**
   * Sync data to persistent storage
   */
  void syncBackendDb();

  /**
   * Close persistent storage
   */
  void closeBackendDb();
}
