package org.fbase.core;

import com.sleepycat.persist.EntityStore;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

import java.util.Map.Entry;
import lombok.extern.log4j.Log4j2;
import org.fbase.config.FBaseConfig;
import org.fbase.config.FileConfig;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.EnumByteExceedException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.handler.MetaModelHandler;
import org.fbase.model.MetaModel;
import org.fbase.handler.MetadataHandler;
import org.fbase.model.function.GBFunction;
import org.fbase.model.output.GroupByColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.TProfile;
import org.fbase.model.profile.CProfile;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.service.*;
import org.fbase.service.impl.*;
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

  /**
   * Get table profile
   * @param uniqueString - Query text or CSV file name
   * @return TProfile - Table profile
   */
  @Override
  public TProfile getTProfile(String uniqueString) {
    TProfile tProfile = new TProfile();
    String tableId = String.valueOf(uniqueString.hashCode());
    tProfile.setTableId(tableId);
    return tProfile;
  }

  @Override
  public TProfile loadJdbcTableMetadata(Connection connection, String select, SProfile sProfile) {
    TProfile tProfile = getTProfile(select);
    String tableId = tProfile.getTableId();

    if (metaModel.getMetadataTables().isEmpty()) {
      saveMetaModel();
    }

    if (!metaModel.getMetadataTables().isEmpty()
        && metaModel.getMetadataTables().get(tableId) != null
        && !metaModel.getMetadataTables().get(tableId).isEmpty()) {

      updateTimestampMetadata(tableId, sProfile);

      tProfile.setIsTimestamp(sProfile.getIsTimestamp());
      tProfile.setCProfiles(getCProfileList(tProfile));

      return tProfile;
    }

    byte tableIdByte = MetaModelHandler.getNextInternalTableId(metaModel);

    try {
      List<CProfile> cProfileList = MetadataHandler.getJdbcCProfileList(connection, select);

      cProfileList.forEach(e -> e.setCsType(sProfile.getCsTypeMap().getOrDefault(e.getColName(),
          new CSType().toBuilder().isTimeStamp(false).sType(SType.RAW).build())));

      metaModel.getMetadataTables().put(tableId, new HashMap<>());
      metaModel.getMetadataTables().get(tableId).putIfAbsent(tableIdByte, cProfileList);
    } catch (Exception e) {
      log.catching(e);
    }

    saveMetaModel();

    return tProfile;
  }

  @Override
  public TProfile loadCsvTableMetadata(String fileName, String csvSplitBy, SProfile sProfile) {
    TProfile tProfile = getTProfile(fileName);
    String tableId = tProfile.getTableId();

    if (metaModel.getMetadataTables().isEmpty()) {
      saveMetaModel();
    }

    if (!metaModel.getMetadataTables().isEmpty()
        && metaModel.getMetadataTables().get(tableId) != null
        && !metaModel.getMetadataTables().get(tableId).isEmpty()) {

      updateTimestampMetadata(tableId, sProfile);

      tProfile.setIsTimestamp(sProfile.getIsTimestamp());
      tProfile.setCProfiles(getCProfileList(tProfile));

      saveMetaModel();

      return tProfile;
    }

    byte tableIdByte = MetaModelHandler.getNextInternalTableId(metaModel);

    try {
      if (sProfile.getCsTypeMap().isEmpty()) {
        MetadataHandler.loadMetadataFromCsv(fileName, csvSplitBy, sProfile);
      }

      List<CProfile> cProfileList = MetadataHandler.getCsvCProfileList(sProfile);

      metaModel.getMetadataTables().put(tableId, new HashMap<>());
      metaModel.getMetadataTables().get(tableId).putIfAbsent(tableIdByte, cProfileList);
    } catch (Exception e) {
      log.catching(e);
    }

    saveMetaModel();

    return tProfile;
  }

  private void updateTimestampMetadata(String tableId, SProfile sProfile) {
      Optional<CProfile> optionalTsCProfile = metaModel.getMetadataTables().get(tableId)
          .entrySet()
          .stream()
          .findAny()
          .orElseThrow()
          .getValue()
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
        byte tableIdByte = metaModel.getMetadataTables().get(tableId).keySet().stream().findAny()
            .orElseThrow();

        for (CProfile cProfile : metaModel.getMetadataTables().get(tableId).get(tableIdByte)) {
          if (cProfile != null && optionalTsEntry.get().getKey().equals(cProfile.getColName())) {
            cProfile.getCsType().setTimeStamp(true);
            break;
          }
        }
      }

    if (optionalTsCProfile.isEmpty() & optionalTsEntry.isEmpty() & sProfile.getIsTimestamp()) {
      throw new RuntimeException("Timestamp column not defined");
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

  @Override
  public List<CProfile> getCProfileList(TProfile tProfile) {
    Map<Byte, List<CProfile>> collection = metaModel.getMetadataTables().get(tProfile.getTableId());

    if (collection != null && !collection.isEmpty()) {
      return collection.values()
              .stream()
              .findAny()
              .orElse(Collections.emptyList());
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void putDataDirect(TProfile tProfile, List<List<Object>> data) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadataTables().get(tProfile.getTableId()).isEmpty()) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataDirect(tProfile, data);
  }

  @Override
  public long putDataJdbc(TProfile tProfile, ResultSet resultSet) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadataTables().get(tProfile.getTableId()).isEmpty()) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    return this.storeService.putDataJdbc(tProfile, resultSet);
  }
  @Override
  public void putDataJdbcBatch(TProfile tProfile, ResultSet resultSet, Integer fBaseBatchSize) throws SqlColMetadataException, EnumByteExceedException {

    if (this.metaModel.getMetadataTables().get(tProfile.getTableId()).isEmpty()) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataJdbcBatch(tProfile, resultSet, fBaseBatchSize);
  }

  @Override
  public void putDataCsvBatch(TProfile tProfile, String fileName, String csvSplitBy, Integer fBaseBatchSize) throws SqlColMetadataException {

    if (this.metaModel.getMetadataTables().get(tProfile.getTableId()).isEmpty()) {
      throw new SqlColMetadataException("Empty sql column metadata for FBase instance..");
    }

    this.storeService.putDataCsvBatch(tProfile, fileName, csvSplitBy, fBaseBatchSize);
  }

  @Override
  public List<StackedColumn> getSColumnListByCProfile(TProfile tProfile, CProfile cProfile,
      long begin, long end) throws SqlColMetadataException, BeginEndWrongOrderException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    if (cProfile.getCsType().getSType() == SType.HISTOGRAM) {
      return this.metadataService.getListStackedColumn(tProfile, cProfile, begin, end);
    } else if (cProfile.getCsType().getSType() == SType.ENUM) {
      return this.enumService.getListStackedColumn(tProfile, cProfile, begin, end);
    } else {
      return this.rawService.getListStackedColumn(tProfile, cProfile, begin, end);
    }
  }

  @Override
  public List<GanttColumn> getGColumnListTwoLevelGroupBy(TProfile tProfile,
      CProfile firstLevelGroupBy, CProfile secondLevelGroupBy, long begin, long end)
      throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {

    if (begin > end) {
      throw new BeginEndWrongOrderException("Begin value must be less the end one..");
    }

    log.info("First column profile: " + firstLevelGroupBy);
    log.info("Second column profile: " + secondLevelGroupBy);

    if (firstLevelGroupBy.getCsType().getSType().equals(SType.HISTOGRAM) &
        secondLevelGroupBy.getCsType().getSType().equals(SType.HISTOGRAM)) {
      return this.metadataService.getListGanttColumn(tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
    } else {
      return this.groupByService.getListGanttColumnUniversal(tProfile, firstLevelGroupBy, secondLevelGroupBy, begin, end);
    }

  }

  @Override
  public List<GroupByColumn> getGBColumnListTwoLevelGroupBy(TProfile tProfile, CProfile firstLevelGroupBy,
      CProfile secondLevelGroupBy, GBFunction gbFunction, long begin, long end)
      throws SqlColMetadataException, BeginEndWrongOrderException, GanttColumnNotSupportedException {
    return null;
  }

  @Override
  public List<List<Object>> getRawDataByColumn(TProfile tProfile, CProfile cProfile, long begin, long end) {
    return rawService.getRawDataByColumn(tProfile, cProfile, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(TProfile tProfile, long begin, long end) {
    return rawService.getRawDataAll(tProfile, begin, end);
  }

  @Override
  public List<List<Object>> getRawDataAll(TProfile tProfile) {
    return rawService.getRawDataAll(tProfile);
  }

  @Override
  public long getLastTimestamp(TProfile tProfile, long begin, long end) {
    return metadataService.getLastTimestamp(tProfile, begin, end);
  }

  @Override
  public void syncBackendDb() {}

  @Override
  public void closeBackendDb() {}

}
