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
import org.fbase.storage.pqsql.DimensionPgSqlImpl;
import org.fbase.storage.pqsql.EnumPgSqlImpl;
import org.fbase.storage.pqsql.HistogramPgSqlImpl;
import org.fbase.storage.pqsql.RawPgSqlImpl;

@Log4j2
public class PgSqlStore extends CommonStore implements FStore {

  private final BasicDataSource basicDataSource;

  public PgSqlStore(FBaseConfig fBaseConfig,
                    BasicDataSource basicDataSource) {
    super(fBaseConfig);

    this.basicDataSource = basicDataSource;

    this.rawDAO = new RawPgSqlImpl(this.metaModelApi, this.basicDataSource);
    this.enumDAO = new EnumPgSqlImpl(this.basicDataSource);
    this.histogramDAO = new HistogramPgSqlImpl(this.basicDataSource);
    this.dimensionDAO = new DimensionPgSqlImpl(this.basicDataSource);

    this.converter = new Converter(dimensionDAO);

    this.histogramsService = new HistogramServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO);
    this.rawService = new RawServiceImpl(this.metaModelApi, converter, rawDAO, histogramDAO, enumDAO);
    this.enumService = new EnumServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO);
    this.groupByService = new GroupByServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);
    this.groupByOneService = new GroupByOneServiceImpl(this.metaModelApi, converter, histogramDAO, rawDAO, enumDAO);

    this.storeService = new StoreServiceImpl(this.metaModelApi, converter, rawDAO, enumDAO, histogramDAO);
  }
}
