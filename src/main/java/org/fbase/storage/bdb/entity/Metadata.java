package org.fbase.storage.bdb.entity;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Metadata {

  @PrimaryKey
  private ColumnKey key;

  private byte[] dataType;
  private byte[] storageType;

  private int[] histograms;
}
