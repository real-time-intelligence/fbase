package org.fbase.handler;

import lombok.extern.log4j.Log4j2;
import org.fbase.model.MetaModel;

@Log4j2
public class MetaModelHandler {

  public static byte getNextInternalTableId(MetaModel metaModel) {
    return metaModel.getMetadata().entrySet().isEmpty() ? Byte.MIN_VALUE : (byte) (
        metaModel.getMetadata().entrySet().stream()
                .max((entry1, entry2) ->
                    entry1.getValue().getTableId() >
                        entry2.getValue().getTableId() ? 1 : -1)
                .get()
                .getValue().getTableId() + 1);
  }
}
