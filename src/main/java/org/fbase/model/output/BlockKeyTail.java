package org.fbase.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class BlockKeyTail implements Comparable<BlockKeyTail>{

  @EqualsAndHashCode.Include
  private long key;

  private long tail;

  @Override
  public int compareTo(BlockKeyTail o) {
    return Long.compare(this.key, o.key);
  }
}
