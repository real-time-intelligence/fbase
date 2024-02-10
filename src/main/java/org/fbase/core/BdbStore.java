package org.fbase.core;

import com.sleepycat.persist.EntityStore;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.fbase.config.FBaseConfig;
import org.fbase.config.FileConfig;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.handler.MetaModelHandler;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.MetaModel;
import org.fbase.model.MetaModel.TableMetadata;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.TType;
import org.fbase.service.*;
import org.fbase.service.impl.*;
import org.fbase.service.mapping.Mapper;
import org.fbase.sql.BatchResultSet;
import org.fbase.storage.Converter;
import org.fbase.storage.DimensionDAO;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.impl.DimensionBdbImpl;
import org.fbase.storage.bdb.impl.EnumBdbImpl;
import org.fbase.storage.bdb.impl.HistogramBdbImpl;
import org.fbase.storage.bdb.impl.RawBdbImpl;

@Log4j2
public class BdbStore implements FStore {

  private final EntityStore store;

  private final MetaModel metaModel;
  private final FileConfig fileConfig;

  private final HistogramDAO histogramDAO;
  private final RawDAO rawDAO;
  private final DimensionDAO dimensionDAO;
  private final EnumDAO enumDAO;

  private final GroupByService groupByService;
  private final GroupByOneService groupByOneService;
  private final HistogramService histogramsService;
  private final RawService rawService;
  private final StoreService storeService;
  private final EnumService enumService;
  private final Converter converter;

  public BdbStore(FBaseConfig fBaseConfig,
                  EntityStore store) {
    this.store = store;

    this.fileConfig = new FileConfig(fBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

    this.rawDAO = new RawBdbImpl(this.store);
    this.enumDAO = new EnumBdbImpl(this.store);
    this.histogramDAO = new HistogramBdbImpl(this.store);
    this.dimensionDAO = new DimensionBdbImpl(this.store);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(metaModel, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(metaModel, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(metaModel, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(metaModel, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(metaModel, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(metaModel, converter, rawDAO, enumDAO, histogramDAO);
  }

  @Override
  public TProfile getTProfile(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(tableName) || tableName.isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }

    TProfile tProfile = new TProfile().setTableName(tableName);

    List<CProfile> cProfiles = getCProfileList(tableName);

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
  public TProfile loadCsvTableMetadata(String fileName,
                                       String csvSplitBy,
                                       SProfile sProfile)
      throws TableNameEmptyException {
    checkAndInitializeMetaModel();
    return loadTableMetadata(sProfile,
                             () -> fetchMetadataFromCsv(fileName, csvSplitBy, sProfile),
                             () -> {});
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

  private boolean metaModelColumModelNotTheSame(String tableName, SProfile sProfile) {
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
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));
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
          .setCompression(sProfile.getCompression())
          .setCProfiles(cProfileList));
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

  private List<CProfile> getCProfileList(String tableName) throws TableNameEmptyException {
    if (Objects.isNull(metaModel.getMetadata().get(tableName))) {
      log.warn("Metamodel for table name: " + tableName + " not found");
      return Collections.emptyList();
    }

    if (Objects.isNull(metaModel.getMetadata().get(tableName).getCProfiles())) {
      throw new TableNameEmptyException("Metamodel for table name: " + tableName + " not found");
    }

    return metaModel.getMetadata().get(tableName).getCProfiles();
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
  public List<StackedColumn> getSColumnListByCProfile(String tableName,
                                                      CProfile cProfile,
                                                      long begin,
                                                      long end)
      throws SqlColMetadataException, BeginEndWrongOrderException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    return this.groupByOneService.getListStackedColumn(tableName, cProfile, begin, end);
  }

  @Override
  public List<StackedColumn> getSColumnListByCProfileFilter(String tableName,
                                                            CProfile cProfile,
                                                            CProfile cProfileFilter,
                                                            String filter,
                                                            long begin,
                                                            long end)
      throws SqlColMetadataException, BeginEndWrongOrderException {
    return this.groupByOneService.getListStackedColumnFilter(tableName, cProfile, cProfileFilter, filter, begin, end);
  }

  @Override
  public List<GanttColumn> getGColumnListTwoLevelGroupBy(String tableName,
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
  public long getLastTimestamp(String tableName,
                               long begin,
                               long end) {
    return rawService.getLastTimestamp(tableName, begin, end);
  }

  @Override
  public void syncBackendDb() {
  }

  @Override
  public void closeBackendDb() {
  }
}
