package org.fbase.model.profile.cstype;

import java.io.Serializable;
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
public class CSType implements Serializable {

  @EqualsAndHashCode.Include
  private int colId;

  private boolean isTimeStamp;

  private SType sType;

  private CType cType;
}
