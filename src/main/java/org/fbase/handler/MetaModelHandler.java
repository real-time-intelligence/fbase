package org.fbase.handler;

import lombok.extern.log4j.Log4j2;
import org.fbase.model.MetaModel;

@Log4j2
public class MetaModelHandler {

  public static byte getNextInternalTableId(MetaModel metaModel) {
    return metaModel.getMetadataTables().entrySet().isEmpty() ? Byte.MIN_VALUE : (byte) (
        metaModel.getMetadataTables().entrySet().stream()
                .max((entry1, entry2) ->
                    entry1.getValue().entrySet().stream().findFirst().get().getKey() >
                        entry2.getValue().entrySet().stream().findFirst().get().getKey() ? 1 : -1)
                .get()
                .getValue().entrySet().stream().findFirst().get().getKey() + 1);
  }
}
