package org.fbase.core;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.fbase.config.FBaseConfig;
import org.fbase.service.impl.EnumServiceImpl;
import org.fbase.service.impl.GroupByOneServiceImpl;
import org.fbase.service.impl.GroupByServiceImpl;
import org.fbase.service.impl.HistogramServiceImpl;
import org.fbase.service.impl.RawServiceImpl;
import org.fbase.service.impl.StoreServiceImpl;
import org.fbase.storage.Converter;
import org.fbase.storage.ch.DimensionChImpl;
import org.fbase.storage.ch.EnumChImpl;
import org.fbase.storage.ch.HistogramChImpl;
import org.fbase.storage.ch.RawChImpl;

@Log4j2
public class ChStore extends CommonStore implements FStore {

  private final BasicDataSource basicDataSource;

  public ChStore(FBaseConfig fBaseConfig,
                 BasicDataSource basicDataSource) {
    super(fBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawChImpl(this.metaModelApi, this.basicDataSource);
    this.enumDAO = new EnumChImpl(this.basicDataSource);
    this.histogramDAO = new HistogramChImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionChImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(metaModel, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(metaModel, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(metaModel, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(metaModel, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(metaModel, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(metaModel, converter, rawDAO, enumDAO, histogramDAO);
  }
}
