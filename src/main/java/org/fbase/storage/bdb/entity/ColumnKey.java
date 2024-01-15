package org.fbase.storage.bdb.entity;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Persistent
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ColumnKey {

  @KeyField(1)
  private byte tableId;

  @KeyField(2)
  private long blockId;

  @KeyField(3)
  private int colId;
}
