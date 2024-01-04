package org.fbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.fbase.common.AbstractDirectTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.metadata.DataType;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FBase11MapArrayTest extends AbstractDirectTest {

  @BeforeAll
  public void init() {
    SProfile sProfile = new SProfile();
    sProfile.setTableName(tableName);
    sProfile.setTableType(TType.TIME_SERIES);
    sProfile.setIndexType(IType.GLOBAL);
    sProfile.setCompression(true);

    Map<String, CSType> csTypeMap = new LinkedHashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("MESSAGE", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.VARCHAR).build());
    csTypeMap.put("MAP", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.MAP).build());

    sProfile.setCsTypeMap(csTypeMap);

    putDataDirect(sProfile);
  }

  @Test
  public void computeStackedTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listMessage = getDataStackedColumn("MESSAGE", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<StackedColumn> listMap = getDataStackedColumn("MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(firstListStackedKey(listMessage), testMessage);
    assertEquals(firstListStackedValue(listMessage), 2);

    Map<String, Integer> integerMapLocal = new HashMap<>(integerMap);
    integerMapLocal.replaceAll((key, value) -> value * kMap);

    assertEquals(integerMapLocal, listMap.get(0).getKeyCount());
  }

  @Test
  public void computeGanttTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    List<GanttColumn> actualMapMessage = getDataGanttColumn("MAP", "MESSAGE", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualMessageMap = getDataGanttColumn("MESSAGE", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);


  }

  @Test
  public void computeRawTest() {
    List<List<Object>> actual = getRawDataAll(Integer.MIN_VALUE, Integer.MAX_VALUE);
    assertEquals(testMessage, actual.get(0).get(1));
  }
}
