package org.fbase.storage.bdb.entity.column;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.fbase.metadata.CompressType;
import org.fbase.storage.bdb.entity.ColumnKey;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class EColumn {

  @PrimaryKey
  private ColumnKey columnKey;

  private CompressType compressionType = CompressType.NONE;

  private int[] values;

  private byte[] dataByte;
}
