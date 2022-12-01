package org.fbase.storage.bdb.entity.raw;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.fbase.storage.bdb.entity.ColumnKey;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RMapping {

  @PrimaryKey
  private ColumnKey key;
}
