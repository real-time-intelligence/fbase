package org.fbase.model.profile;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.SType;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class SProfile {

  private Map<String, CSType> csTypeMap;
}
