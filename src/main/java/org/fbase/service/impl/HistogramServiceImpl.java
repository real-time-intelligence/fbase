package org.fbase.service.impl;

import lombok.extern.log4j.Log4j2;
import org.fbase.core.Converter;
import org.fbase.model.MetaModel;
import org.fbase.service.CommonServiceApi;
import org.fbase.service.HistogramsService;
import org.fbase.storage.HistogramDAO;

@Log4j2
public class HistogramServiceImpl extends CommonServiceApi implements HistogramsService {

  private final MetaModel metaModel;
  private final Converter converter;
  private final HistogramDAO histogramDAO;

  public HistogramServiceImpl(MetaModel metaModel, Converter converter, HistogramDAO histogramDAO) {
    this.metaModel = metaModel;
    this.converter = converter;
    this.histogramDAO = histogramDAO;
  }

}
