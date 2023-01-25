package org.fbase.core;

import com.sleepycat.persist.EntityStore;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.fbase.config.FBaseConfig;
import org.fbase.config.FileConfig;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.exception.TableNameEmptyException;
import org.fbase.handler.MetaModelHandler;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.MetaModel;
import org.fbase.model.MetaModel.TableMetadata;
import org.fbase.model.function.GBFunction;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.GroupByColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.EnumService;
import org.fbase.service.GroupByService;
import org.fbase.service.HistogramsService;
import org.fbase.service.MetadataService;
import org.fbase.service.RawService;
import org.fbase.service.StoreService;
import org.fbase.service.impl.EnumServiceImpl;
import org.fbase.service.impl.GroupByServiceImpl;
import org.fbase.service.impl.HistogramServiceImpl;
import org.fbase.service.impl.MetadataServiceImpl;
import org.fbase.service.impl.RawServiceImpl;
import org.fbase.service.impl.StoreServiceImpl;
import org.fbase.storage.DimensionDAO;
import org.fbase.storage.EnumDAO;
import org.fbase.storage.HistogramDAO;
import org.fbase.storage.MetadataDAO;
import org.fbase.storage.RawDAO;
import org.fbase.storage.bdb.impl.DimensionBdbImpl;
import org.fbase.storage.bdb.impl.EnumBdbImpl;
import org.fbase.storage.bdb.impl.HistogramBdbImpl;
import org.fbase.storage.bdb.impl.MetadataBdbImpl;
import org.fbase.storage.bdb.impl.RawBdbImpl;

@Log4j2
public class BdbStore implements FStore {
  private final EntityStore store;

  private final MetaModel metaModel;
  private final FileConfig fileConfig;

  private final HistogramDAO histogramDAO;
  private final MetadataDAO metadataDAO;
  private final RawDAO rawDAO;
  private final DimensionDAO dimensionDAO;
  private final EnumDAO enumDAO;

  private final MetadataService metadataService;
  private final GroupByService groupByService;
  private final HistogramsService histogramsService;
  private final RawService rawService;
  private final StoreService storeService;
  private final EnumService enumService;
  private final Converter converter;

  public BdbStore(FBaseConfig fBaseConfig, EntityStore store) {
    this.store = store;

    this.fileConfig = new FileConfig(fBaseConfig);

    try {
      this.metaModel = fileConfig.readObject() == null ? new MetaModel() : (MetaModel) fileConfig.readObject();
    } catch (IOException | ClassNotFoundException e) {
      log.catching(e);
      throw new RuntimeException(e);
    }

    this.histogramDAO = new HistogramBdbImpl(this.store);
    this.metadataDAO = new MetadataBdbImpl(this.store);
    this.rawDAO = new RawBdbImpl(this.store);
    this.dimensionDAO = new DimensionBdbImpl(this.store);
    this.enumDAO = new EnumBdbImpl(this.store);

    this.converter = new Converter(dimensionDAO);

    this.metadataService = new MetadataServiceImpl(metaModel, converter, metadataDAO, histogramDAO, rawDAO);
    this.histogramsService = new HistogramServiceImpl(metaModel, converter, histogramDAO);
    this.rawService = new RawServiceImpl(metaModel, converter, rawDAO, metadataDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(metaModel, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(metaModel, converter, metadataDAO, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(metaModel, converter, rawDAO, enumDAO, histogramDAO, metadataDAO);
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
              tProfile.setIsTimestamp(true);
              tProfile.setCProfiles(cProfiles);
            },
            () -> {
              tProfile.setIsTimestamp(false);
              tProfile.setCProfiles(cProfiles);
            });

    return tProfile;
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection, String select, SProfile sProfile)
      throws TableNameEmptyException {
    checkIsTableNameEmpty(sProfile);

    TProfile tProfile = new TProfile().setTableName(sProfile.getTableName());
    String tableName = sProfile.getTableName();

    if (metaModel.getMetadata().isEmpty()) {
      saveMetaModel();
    }

    if (!metaModel.getMetadata().isEmpty()
        && metaModel.getMetadata().get(tableName) != null
        && metaModel.getMetadata().get(tableName).getTableId() != null) {

      updateTimestampMetadata(tableName, sProfile);

      tProfile.setIsTimestamp(sProfile.getIsTimestamp());

      try {
        tProfile.setCProfiles(getCProfileList(tableName));
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      saveMetaModel();

      return tProfile;
    }

    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, select);

      cProfileList.forEach(e -> e.setCsType(sProfile.getCsTypeMap().getOrDefault(e.getColName(),
          new CSType().toBuilder().isTimeStamp(false).sType(SType.RAW).build())));

      metaModel.getMetadata().put(tableName,
          new TableMetadata()
              .setTableId(tableId)
              .setCompression(sProfile.getCompression())
              .setCProfiles(cProfileList));

    } catch (Exception e) {
      log.catching(e);
    }

    saveMetaModel();

    return tProfile;
  }

  @Override
  public TProfile loadCsvTableMetadata(String fileName, String csvSplitBy, SProfile sProfile)
      throws TableNameEmptyException {
    checkIsTableNameEmpty(sProfile);

    TProfile tProfile = new TProfile().setTableName(sProfile.getTableName());
    String tableName = sProfile.getTableName();

    if (metaModel.getMetadata().isEmpty()) {
      saveMetaModel();
    }

    if (!metaModel.getMetadata().isEmpty()
        && metaModel.getMetadata().get(tableName) != null
        && metaModel.getMetadata().get(tableName).getTableId() != null) {

      updateTimestampMetadata(tableName, sProfile);

      tProfile.setIsTimestamp(sProfile.getIsTimestamp());
      tProfile.setCompression(sProfile.getCompression());
      try {
        tProfile.setCProfiles(getCProfileList(tableName));
      } catch (TableNameEmptyException e) {
        throw new RuntimeException(e);
      }

      saveMetaModel();

      return tProfile;
    }

    byte tableId = MetaModelHandler.getNextInternalTableId(metaModel);

    try {
      if (sProfile.getCsTypeMap().isEmpty()) {
        MetadataHandler.loadMetadataFromCsv(fileName, csvSplitBy, sProfile);
      }

      List<CProfile> cProfileList = MetadataHandler.getCsvCProfileList(sProfile);

      metaModel.getMetadata().put(tableName,
          new TableMetadata()
              .setTableId(tableId)
              .setCompression(sProfile.getCompression())
              .setCProfiles(cProfileList));

    } catch (Exception e) {
      log.catching(e);
    }

    saveMetaModel();

    tProfile.setIsTimestamp(sProfile.getIsTimestamp());
    tProfile.setCompression(sProfile.getCompression());
    try {
      tProfile.setCProfiles(getCProfileList(tableName));
    } catch (TableNameEmptyException e) {
      throw new RuntimeException(e);
    }

    return tProfile;
  }

  private void checkIsTableNameEmpty(SProfile sProfile) throws TableNameEmptyException {
    if (Objects.isNull(sProfile.getTableName()) || sProfile.getTableName().isBlank()) {
      throw new TableNameEmptyException("Empty table name. Please, define it explicitly..");
    }
  }

  private void updateTimestampMetadata(String tableName, SProfile sProfile) {
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

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isEmpty() & sProfile.getIsTimestamp()) {
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
  public void putDataDirect(String tableName, List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataDirect(tableName, data);
  }

  @Override
  public long putDataJdbc(String tableName, ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    return this.storeService.putDataJdbc(tableName, resultSet);
  }
  @Override
  public void putDataJdbcBatch(String tableName, ResultSet resultSet, Integer fBaseBatchSize) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataJdbcBatch(tableName, resultSet, fBaseBatchSize);
  }

  @Override
  public void putDataCsvBatch(String tableName, String fileName, String csvSplitBy, Integer fBaseBatchSize) throws SqlColMetadataException {

    if (this.metaModel.getMetadata().get(tableName) == null) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataCsvBatch(tableName, fileName, csvSplitBy, fBaseBatchSize);
  }

  @Override
  public List<StackedColumn> getSColumnListByCProfile(String tableName, CProfile cProfile,
      long begin, long end) throws SqlColMetadataException, BeginEndWrongOrderException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    if (cProfile.getCsType().getSType() == SType.HISTOGRAM) {
      return this.metadataService.getListStackedColumn(tableName, cProfile, begin, end);
    } else if (cProfile.getCsType().getSType() == SType.ENUM) {
      return this.enumService.getListStackedColumn(tableName, cProfile, begin, end);
    } else {
      return this.rawService.getListStackedColumn(tableName, cProfile, begin, end);
    }
  }

  @Override
  public List<GanttColumn> getGColumnListTwoLevelGroupBy(String tableName,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstLevelGroupBy);
    log.info("Second column profile: " + secondLevelGroupBy);

    if (firstLevelGroupBy.getCsType().getSType().equals(SType.HISTOGRAM) &
        secondLevelGroupBy.getCsType().getSType().equals(SType.HISTOGRAM)) {
      return this.metadataService.getListGanttColumn(tableName, firstLevelGroupBy, secondLevelGroupBy, begin, end);
    } else {
      return this.groupByService.getListGanttColumnUniversal(tableName, firstLevelGroupBy, secondLevelGroupBy, begin, end);
    }
  }

  @Override
  public List<GroupByColumn> getGBColumnListTwoLevelGroupBy(String tableName, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, GBFunction gbFunction, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {
    return null;
  }

  @Override
  public List<List<Object>> getRawDataByColumn(String tableName, CProfile cProfile, long begin, long end) {
    return rawService.getRawDataByColumn(tableName, cProfile, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName, long begin, long end) {
    return rawService.getRawDataAll(tableName, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(String tableName) {
    return rawService.getRawDataAll(tableName);
  }

  @Override
  public long getLastTimestamp(String tableName, long begin, long end) {
    return metadataService.getLastTimestamp(tableName, begin, end);
  }

  @Override
  public void syncBackendDb() {}

  @Override
  public void closeBackendDb() {}
}
