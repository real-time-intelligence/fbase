package org.fbase.model.profile;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class TProfile {

  private String tableName;
  private TType tableType;
  private IType indexType;
  private Boolean compression;
  private List<CProfile> cProfiles;
}
