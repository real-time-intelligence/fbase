package org.fbase.storage.bdb.entity;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.fbase.metadata.CompressType;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Histogram {

  @PrimaryKey
  private ColumnKey key;

  private CompressType compressionType = CompressType.NONE;

  private int[][] data;

  private byte[] keysCompressed;

  private byte[] valuesCompressed;
}
