package org.fbase.model.profile.cstype;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder(toBuilder = true)
public class CSType implements Serializable {

  private boolean isTimeStamp;
  private SType sType;
  private CType cType;
}
