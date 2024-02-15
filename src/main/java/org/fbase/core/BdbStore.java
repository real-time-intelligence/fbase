package org.fbase.core;

import com.sleepycat.persist.EntityStore;
import lombok.extern.log4j.Log4j2;
import org.fbase.config.FBaseConfig;
import org.fbase.service.impl.EnumServiceImpl;
import org.fbase.service.impl.GroupByOneServiceImpl;
import org.fbase.service.impl.GroupByServiceImpl;
import org.fbase.service.impl.HistogramServiceImpl;
import org.fbase.service.impl.RawServiceImpl;
import org.fbase.service.impl.StoreServiceImpl;
import org.fbase.storage.Converter;
import org.fbase.storage.bdb.impl.DimensionBdbImpl;
import org.fbase.storage.bdb.impl.EnumBdbImpl;
import org.fbase.storage.bdb.impl.HistogramBdbImpl;
import org.fbase.storage.bdb.impl.RawBdbImpl;

@Log4j2
public class BdbStore extends CommonStore implements FStore {

  public BdbStore(FBaseConfig fBaseConfig,
                  EntityStore store) {
    super(fBaseConfig, store);

    this.rawDAO = new RawBdbImpl(this.store);
    this.enumDAO = new EnumBdbImpl(this.store);
    this.histogramDAO = new HistogramBdbImpl(this.store);
    this.dimensionDAO = new DimensionBdbImpl(this.store);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO, histogramDAO);
  }
}
