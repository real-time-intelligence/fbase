package org.fbase;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fbase.common.AbstractH2Test;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.cstype.SType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FBase10StackedBugTest extends AbstractH2Test {

  @BeforeAll
  public void init() {
    Map<String, SType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", SType.RAW);
    csTypeMap.put("FIRSTNAME", SType.RAW);
    csTypeMap.put("LASTNAME", SType.HISTOGRAM);
    csTypeMap.put("HOUSE", SType.ENUM);
    csTypeMap.put("CITY", SType.ENUM);

    putDataDirect(csTypeMap);
  }

  @Test
  public void computeNotEmptyBugTest() {
    cProfiles.forEach(e -> {
      if (!e.getCsType().isTimeStamp()) {
        try {
          failIfEmpty(getDataStackedColumn(e.getColName(), 1, 25));
          failIfEmpty(getDataStackedColumn(e.getColName(), 45722, 48721));
          failIfEmpty(getDataStackedColumn(e.getColName(), 48722, Integer.MAX_VALUE));
          failIfEmpty(getDataStackedColumn(e.getColName(), Integer.MIN_VALUE, Integer.MAX_VALUE));
          failIfEmpty(getDataStackedColumn(e.getColName(), Integer.MIN_VALUE, 0));
        } catch (BeginEndWrongOrderException | SqlColMetadataException ex) {
          throw new RuntimeException(ex);
        }
      }
    });
  }

  private void failIfEmpty(List<StackedColumn> list) {
    for (StackedColumn column : list) {
      Map<?, ?> keyCount = column.getKeyCount();
      assertFalse(keyCount.isEmpty());
    }
  }
}
