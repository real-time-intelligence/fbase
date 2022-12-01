package org.fbase.storage.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class RawDto {

  @EqualsAndHashCode.Include
  private long key;

  private byte[] dataByte;

  private int[] dataInt;

  private long[] dataLong;

  private float[] dataFloat;

  private double[] dataDouble;

  private String[] dataString;
}
