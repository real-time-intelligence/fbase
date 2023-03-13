package org.fbase.model.profile;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class SProfile {

  private String tableName;
  private TType tableType = TType.TIME_SERIES;
  private IType indexType = IType.GLOBAL;
  private Boolean compression = Boolean.FALSE;
  private Map<String, CSType> csTypeMap;
}
