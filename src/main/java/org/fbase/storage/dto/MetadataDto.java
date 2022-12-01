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
public class MetadataDto {

  @EqualsAndHashCode.Include
  private long key;

  private byte[] dataType;
  private byte[] storageType;

  private int[] histograms;
  private int[] enums;
}
