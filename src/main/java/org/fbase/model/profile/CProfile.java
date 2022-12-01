package org.fbase.model.profile;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.fbase.model.profile.cstype.CSType;

@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CProfile implements Serializable {

  @EqualsAndHashCode.Include
  private int colId;
  private int colIdSql;
  private String colName;
  private String colDbTypeName;
  private int colSizeDisplay;
  private int colSizeSqlType;

  private CSType csType;
}
