package org.fbase.core;

import com.sleepycat.persist.EntityStore;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.fbase.config.FBaseConfig;
import org.fbase.config.FileConfig;
import org.fbase.core.metamodel.MetaModelApi;
import org.fbase.core.metamodel.MetaModelApiImpl;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.handler.MetaModelHandler;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.CompareFunction;
import org.fbase.model.GroupFunction;
import org.fbase.model.MetaModel;
import org.fbase.model.MetaModel.TableMetadata;
import org.fbase.model.OrderBy;
import org.fbase.model.output.BlockKeyTail;
import org.fbase.model.output.GanttColumnCount;
import org.fbase.model.output.GanttColumnSum;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
import org.fbase.model.profile.table.TType;
import org.fbase.service.EnumService;
import org.fbase.service.GroupByOneService;
import org.fbase.service.GroupByService;
import org.fbase.service.HistogramService;
import org.fbase.service.RawService;
import org.fbase.service.StatisticsService;
import org.fbase.service.StoreService;
import org.fbase.service.impl.StatisticsServiceImpl;
import org.fbase.service.mapping.Mapper;
import org.fbase.sql.BatchResultSet;
import org.fbase.storage.Converter;
import org.fbase.storage.DimensionDAO;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;

@Log4j2
public abstract class CommonStore implements FStore {

  protected EntityStore store;

  protected MetaModel metaModel;
  protected MetaModelApi metaModelApi;
  protected FileConfig fileConfig;

  protected HistogramDAO histogramDAO;
  protected RawDAO rawDAO;
  protected DimensionDAO dimensionDAO;
  protected EnumDAO enumDAO;

  protected GroupByService groupByService;
  protected GroupByOneService groupByOneService;
  protected HistogramService histogramsService;
  protected RawService rawService;
  protected StoreService storeService;
  protected EnumService enumService;

  protected Converter converter;
  protected StatisticsService statisticsService;

  public CommonStore(FBaseConfig fBaseConfig,
                     EntityStore store) {
    this.store = store;

    this.fileConfig = new FileConfig(fBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
      this.metaModelApi = new MetaModelApiImpl(this.metaModel);
      this.statisticsService = new StatisticsServiceImpl();
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  public CommonStore(FBaseConfig fBaseConfig) {
    this.store = null;

    this.fileConfig = new FileConfig(fBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
      this.metaModelApi = new MetaModelApiImpl(this.metaModel);
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public TProfile getTProfile(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(tableName) || tableName.isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }

    TProfile tProfile = new TProfile().setTableName(tableName);

    checkTableInMetamodel(tableName);

    List<CProfile> cProfiles = metaModel.getMetadata().get(tableName).getCProfiles();

    cProfiles.stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny()
        .ifPresentOrElse((value) -> {
                           tProfile.setTableType(TType.TIME_SERIES);
                           tProfile.setCProfiles(cProfiles);
                         },
                         () -> {
                           tProfile.setTableType(TType.REGULAR);
                           tProfile.setCProfiles(cProfiles);
                         });

    if (Objects.nonNull(metaModel.getMetadata().get(tableName))) {
      MetaModel.TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
      tProfile.setIndexType(tableMetadata.getIndexType());
      tProfile.setBackendType(tableMetadata.getBackendType());
      tProfile.setCompression(tableMetadata.getCompression());
    }

    return tProfile;
  }

  @Override
  public TProfile loadDirectTableMetadata(SProfile sProfile) throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataDirect(sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection,
                                        String select,
                                        SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromJdbc(connection, select, sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection,
                                        String sqlSchemaName,
                                        String sqlTableName,
                                        SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromJdbc(connection, sqlSchemaName, sqlTableName, sProfile),
                             () -> updateTimestampMetadata(sProfile.getTableName(), sProfile));
  }

  @Override
  public TProfile loadCsvTableMetadata(String fileName,
                                       String csvSplitBy,
                                       SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromCsv(fileName, csvSplitBy, sProfile),
                             () -> {
                             });
  }

  @Override
  public void setTimestampColumn(String tableName,
                                 String timestampColumnName)
      throws TableNameEmptyException {

    checkTableInMetamodel(tableName);

    if (timestampColumnName.isBlank()) {
      log.warn("Empty timestamp column name");
      return;
    }

    Optional<CProfile> optionalTsCProfile = metaModel.getMetadata().get(tableName)
        .getCProfiles()
        .stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny();

    log.info("Set timestamp column: " + timestampColumnName + " for table: " + tableName);

    if (optionalTsCProfile.isPresent()) {
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && optionalTsCProfile.get().getColName().equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(false);
        }

        if (cProfile != null && timestampColumnName.equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
        }
      }
    } else {
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && timestampColumnName.equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
          break;
        }
      }
    }
  }

  private void checkAndInitializeMetaModel() {
    if (metaModel.getMetadata().isEmpty()) {
      saveMetaModel();
    }
  }

  private TProfile loadTableMetadata(SProfile sProfile,
                                     Runnable fetchMetadata,
                                     Runnable updateTimestampMetadata)
      throws TableNameEmptyException {
    checkIsTableNameEmpty(sProfile);

    String tableName = sProfile.getTableName();
    TProfile tProfile = new TProfile().setTableName(tableName);

    if (metaModelExistsForTable(tableName)) {
      if (metaModelColumModelNotTheSame(tableName, sProfile)) {
        fetchMetadata.run();
      }
      updateTimestampMetadata.run();
      fillTProfileFromMetaModel(tableName, tProfile);
    } else {
      fetchMetadata.run();
      fillTProfileAndSaveMetaModel(tableName, sProfile, tProfile);
    }

    saveMetaModel();

    return tProfile;
  }

  private boolean metaModelExistsForTable(String tableName) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    return tableMetadata != null && tableMetadata.getTableId() != null;
  }

  private boolean metaModelColumModelNotTheSame(String tableName,
                                                SProfile sProfile) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    return tableMetadata.getCProfiles().size() != sProfile.getCsTypeMap().size();
  }

  private void fetchMetadataDirect(SProfile sProfile) {
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    if (sProfile.getCsTypeMap().isEmpty()) {
      throw new RuntimeException("Storage profile is empty");
    }

    try {
      List<CProfile> cProfileList = MetadataHandler.getDirectCProfileList(sProfile);

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }

    } catch (Exception e) {
      log.catching(e);
    }
  }

  private void putMetadataNonBdb(SProfile sProfile,
                                 byte tableId,
                                 List<CProfile> cProfileList) {
    try {
      List<Byte> rawCTypeKeys = cProfileList.stream()
          .filter(cProfile -> SType.RAW.equals(cProfile.getCsType().getSType()))
          .map(cProfile -> cProfile.getCsType().getCType().getKey())
          .collect(Collectors.toList());

      int[] rawColIds = cProfileList.stream()
          .filter(cProfile -> SType.RAW.equals(cProfile.getCsType().getSType()))
          .mapToInt(CProfile::getColId)
          .toArray();

      if (cProfileList.size() != rawCTypeKeys.size()) {
        throw new RuntimeException("Not supported for backend: " + sProfile.getBackendType());
      }

      this.rawDAO.putMetadata(tableId, 0L, getByteFromList(rawCTypeKeys), rawColIds, new int[0], new int[0]);

    } catch (Exception e) {
      log.catching(e);
    }
  }

  public byte[] getByteFromList(List<Byte> list) {
    byte[] byteArray = new byte[list.size()];
    int index = 0;
    for (byte b : list) {
      byteArray[index++] = b;
    }
    return byteArray;
  }

  private void fetchMetadataFromJdbc(Connection connection,
                                     String sqlSchemaName,
                                     String sqlTableName,
                                     SProfile sProfile) {
    if (!BType.ORACLE.equals(sProfile.getBackendType())) {
      throw new RuntimeException("Not supported backend type: " + sProfile.getBackendType());
    }
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, sqlSchemaName, sqlTableName);

      Map<String, CSType> csTypeMap = sProfile.getCsTypeMap();

      cProfileList.forEach(cProfile -> {
        CSType csType = csTypeMap.getOrDefault(cProfile.getColName(), defaultCSType());
        csType.setCType(Mapper.isCType(cProfile));
        csType.setDType(Mapper.isDBType(cProfile));
        cProfile.setCsType(csType);
        log.info(cProfile);
      });

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }
    } catch (Exception e) {
      log.catching(e);
    }
  }

  private void fetchMetadataFromJdbc(Connection connection,
                                     String select,
                                     SProfile sProfile) {
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, select);

      Map<String, CSType> csTypeMap = sProfile.getCsTypeMap();

      cProfileList.forEach(cProfile -> {
        CSType csType = csTypeMap.getOrDefault(cProfile.getColName(), defaultCSType());
        csType.setCType(Mapper.isCType(cProfile));
        csType.setDType(Mapper.isDBType(cProfile));
        cProfile.setCsType(csType);
        log.info(cProfile);
      });

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));

      if (!BType.BERKLEYDB.equals(sProfile.getBackendType())) {
        putMetadataNonBdb(sProfile, tableId, cProfileList);
      }
    } catch (Exception e) {
      log.catching(e);
    }
  }

  private CSType defaultCSType() {
    return new CSType().toBuilder()
        .isTimeStamp(false)
        .sType(SType.RAW)
        .build();
  }

  private void fetchMetadataFromCsv(String fileName,
                                    String csvSplitBy,
                                    SProfile sProfile) {
    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);
    String tableName = sProfile.getTableName();

    try {
      if (sProfile.getCsTypeMap().isEmpty()) {
        MetadataHandler.loadMetadataFromCsv(fileName, csvSplitBy, sProfile);
      }
      List<CProfile> cProfileList = MetadataHandler.getCsvCProfileList(sProfile);

      metaModel.getMetadata().put(tableName, new TableMetadata()
          .setTableId(tableId)
          .setTableType(sProfile.getTableType())
          .setIndexType(sProfile.getIndexType())
          .setBackendType(sProfile.getBackendType())
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));
    } catch (Exception e) {
      log.catching(e);
    }
  }

  private void fillTProfileAndSaveMetaModel(String tableName,
                                            SProfile sProfile,
                                            TProfile tProfile) {
    tProfile.setTableType(sProfile.getTableType());
    tProfile.setCompression(sProfile.getCompression());

    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);

    if (tableMetadata != null) {
      List<CProfile> cProfiles = tableMetadata.getCProfiles();
      tProfile.setCProfiles(cProfiles);
    }

    saveMetaModel();
  }

  private void fillTProfileFromMetaModel(String tableName,
                                         TProfile tProfile) {
    TableMetadata tableMetadata = metaModel.getMetadata().get(tableName);
    if (tableMetadata != null) {
      List<CProfile> cProfileList = tableMetadata.getCProfiles();

      tProfile.setTableType(tableMetadata.getTableType());
      tProfile.setIndexType(tableMetadata.getIndexType());
      tProfile.setBackendType(tableMetadata.getBackendType());
      tProfile.setCompression(tableMetadata.getCompression());
      tProfile.setCProfiles(cProfileList);
    } else {
      log.warn("No metadata found for table: " + tableName);
    }
  }

  private void checkIsTableNameEmpty(SProfile sProfile) throws TableNameEmptyException {
    if (Objects.isNull(sProfile.getTableName()) || sProfile.getTableName().isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }
  }

  private void updateTimestampMetadata(String tableName,
                                       SProfile sProfile) {
    Optional<CProfile> optionalTsCProfile = metaModel.getMetadata().get(tableName)
        .getCProfiles()
        .stream()
        .filter(cProfile -> cProfile.getCsType().isTimeStamp())
        .findAny();

    Optional<Entry<String, CSType>> optionalTsEntry = sProfile.getCsTypeMap().entrySet()
        .stream()
        .filter(entry -> Objects.nonNull(entry.getValue()))
        .filter(entry -> entry.getValue().isTimeStamp())
        .findAny();

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isPresent()) {
      log.info("Update timestamp column in FBase metadata");
      for (CProfile cProfile : metaModel.getMetadata().get(tableName).getCProfiles()) {
        if (cProfile != null && optionalTsEntry.get().getKey().equals(cProfile.getColName())) {
          cProfile.getCsType().setTimeStamp(true);
          break;
        }
      }
    }

    metaModel.getMetadata().get(tableName).setTableType(sProfile.getTableType());
    metaModel.getMetadata().get(tableName).setIndexType(sProfile.getIndexType());
    metaModel.getMetadata().get(tableName).setBackendType(sProfile.getBackendType());
    metaModel.getMetadata().get(tableName).setCompression(sProfile.getCompression());

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isEmpty()
        & !TType.TIME_SERIES.equals(sProfile.getTableType())) {
      log.warn("Timestamp column not defined");
    }
  }

  private void saveMetaModel() {
    try {
      fileConfig.saveObject(metaModel);
    } catch (IOException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }
  }

  private void checkTableInMetamodel(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(metaModel.getMetadata().get(tableName))) {
      log.warn("Metamodel for table name: " + tableName + " not found");
    }

    if (Objects.isNull(metaModel.getMetadata().get(tableName).getCProfiles())) {
      throw new TableNameEmptyException("Metamodel for table name: " + tableName + " not found");
    }
  }

  @Override
  public void putDataDirect(String tableName,
                            List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataDirect(tableName, data);
  }

  @Override
  public long putDataJdbc(String tableName,
                          ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    return this.storeService.putDataJdbc(tableName, resultSet);
  }

  @Override
  public void putDataJdbcBatch(String tableName,
                               ResultSet resultSet,
                               Integer fBaseBatchSize) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataJdbcBatch(tableName, resultSet, fBaseBatchSize);
  }

  @Override
  public void putDataCsvBatch(String tableName,
                              String fileName,
                              String csvSplitBy,
                              Integer fBaseBatchSize) throws SqlColMetadataException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    if (fBaseBatchSize <= 0) {
      log.warn("Batch size can not be less or equal 0. Set to the default value of 1");
      fBaseBatchSize = 1;
    }

    this.storeService.putDataCsvBatch(tableName, fileName, csvSplitBy, fBaseBatchSize);
  }

  @Override
  public List<BlockKeyTail> getBlockKeyTailList(String tableName,
                                                long begin,
                                                long end) throws BeginEndWrongOrderException, SqlColMetadataException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return rawService.getBlockKeyTailList(tableName, begin, end);
  }

  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        long begin,
                                        long end)
      throws SqlColMetadataException, BeginEndWrongOrderException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return this.groupByOneService.getListStackedColumn(tableName, cProfile, groupFunction, begin, end);
  }


  @Override
  public List<StackedColumn> getStacked(String tableName,
                                        CProfile cProfile,
                                        GroupFunction groupFunction,
                                        CProfile cProfileFilter,
                                        String[] filterData,
                                        CompareFunction compareFunction,
                                        long begin,
                                        long end)
      throws SqlColMetadataException, BeginEndWrongOrderException {
    return this.groupByOneService.getListStackedColumnFilter(tableName, cProfile, groupFunction, cProfileFilter, filterData, compareFunction, begin, end);
  }

  @Override
  public List<GanttColumnCount> getGantt(String tableName,
                                         CProfile firstGrpBy,
                                         CProfile secondGrpBy,
                                         long begin,
                                         long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {

    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getListGanttColumn(tableName, firstGrpBy, secondGrpBy, begin, end);
  }

  @Override
  public List<GanttColumnCount> getGantt(String tableName,
                                         CProfile firstGrpBy,
                                         CProfile secondGrpBy,
                                         int batchSize,
                                         long begin,
                                         long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {

    BType backendType = metaModel.getMetadata().get(tableName).getBackendType();

    if (!BType.BERKLEYDB.equals(backendType)) {
      throw new RuntimeException("Not supported for backend type: " + backendType);
    }

    return groupByService.getListGanttColumn(tableName, firstGrpBy, secondGrpBy, batchSize, begin, end);
  }

  @Override
  public List<GanttColumnCount> getGantt(String tableName,
                                         CProfile firstGrpBy,
                                         CProfile secondGrpBy,
                                         CProfile cProfileFilter,
                                         String[] filterData,
                                         CompareFunction compareFunction,
                                         long begin,
                                         long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {

    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getListGanttColumn(tableName, firstGrpBy, secondGrpBy,
                                                  cProfileFilter, filterData, compareFunction, begin, end);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          long begin,
                                          long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {
    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getListGanttSumColumn(tableName, firstGrpBy, secondGrpBy, begin, end);
  }

  @Override
  public List<GanttColumnSum> getGanttSum(String tableName,
                                          CProfile firstGrpBy,
                                          CProfile secondGrpBy,
                                          CProfile cProfileFilter,
                                          String[] filterData,
                                          CompareFunction compareFunction,
                                          long begin,
                                          long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {
    if (firstGrpBy.getCsType().isTimeStamp() | secondGrpBy.getCsType().isTimeStamp()) {
      throw new SqlColMetadataException("Group by not supported for timestamp column..");
    }

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstGrpBy);
    log.info("Second column profile: " + secondGrpBy);

    return this.groupByService.getListGanttSumColumn(tableName, firstGrpBy, secondGrpBy,
                                                     cProfileFilter, filterData, compareFunction, begin, end);
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  int limit,
                                  long begin,
                                  long end)
      throws BeginEndWrongOrderException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return groupByService.getDistinct(tableName, cProfile, orderBy, limit, begin, end);
  }

  @Override
  public List<String> getDistinct(String tableName,
                                  CProfile cProfile,
                                  OrderBy orderBy,
                                  int limit,
                                  long begin,
                                  long end,
                                  CProfile cProfileFilter,
                                  String[] filterData,
                                  CompareFunction compareFunction)
      throws BeginEndWrongOrderException {
    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return groupByService.getDistinct(tableName, cProfile, orderBy, limit, begin, end, cProfileFilter, filterData, compareFunction);
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName,
                                               CProfile cProfile,
                                               long begin,
                                               long end) {
    return rawService.getRawDataByColumn(tableName, cProfile, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          long begin,
                                          long end) {
    return rawService.getRawDataAll(tableName, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName,
                                          CProfile cProfileFilter,
                                          String filter,
                                          long begin,
                                          long end) {
    return rawService.getRawDataAll(tableName, cProfileFilter, filter, begin, end);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          int fetchSize) {
    if (fetchSize <= 0) {
      log.warn("Fetch size can not be less or equal 0. Set to the default value of 1");
      fetchSize = 1;
    }

    return rawService.getBatchResultSet(tableName, 0L, Long.MAX_VALUE, fetchSize);
  }

  @Override
  public BatchResultSet getBatchResultSet(String tableName,
                                          long begin,
                                          long end,
                                          int fetchSize) {
    if (fetchSize <= 0) {
      log.warn("Fetch size can not be less or equal 0. Set to the default value of 1");
      fetchSize = 1;
    }

    return rawService.getBatchResultSet(tableName, begin, end, fetchSize);
  }

  @Override
  public long getFirst(String tableName,
                       long begin,
                       long end) {
    return rawService.getFirst(tableName, begin, end);
  }

  @Override
  public long getLast(String tableName,
                      long begin,
                      long end) {
    return rawService.getLast(tableName, begin, end);
  }

  @Override
  public void syncBackendDb() {
  }

  @Override
  public void closeBackendDb() {
  }
}
