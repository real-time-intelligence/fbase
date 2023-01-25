package org.fbase.model.profile;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class TProfile {

  private String tableName;
  private Boolean isTimestamp;
  private Boolean compression;
  private List<CProfile> cProfiles;
}
