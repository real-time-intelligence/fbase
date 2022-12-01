package org.fbase.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.fbase.model.profile.CProfile;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
public class MetaModel implements Serializable {

  private Map<String, Map<Byte, List<CProfile>>> metadataTables = new HashMap<>();
}
