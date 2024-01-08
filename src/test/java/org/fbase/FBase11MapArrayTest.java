package org.fbase;

import static org.fbase.service.CommonServiceApi.transpose;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
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

    Map<String, CSType> csTypeMap = new HashMap<>();
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

    assertEquals(findListStackedKey(listMessage, testMessage1), testMessage1);
    assertEquals(findListStackedValue(listMessage, testMessage1), 2);

    assertEquals(findListStackedKey(listMessage, testMessage2), testMessage2);
    assertEquals(findListStackedValue(listMessage, testMessage2), 2);

    compareKeySetForMapDataType(testMap1, listMap);
    compareKeySetForMapDataType(testMap2, listMap);
    assertEquals(testMap3, listMap.stream().filter(f -> f.getKeyCount().isEmpty()).findAny().orElseThrow().getKeyCount());
  }

  @Test
  public void computeGanttTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    List<GanttColumn> actualMapMessage = getDataGanttColumn("MAP", "MESSAGE", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualMessageMap = getDataGanttColumn("MESSAGE", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualMapMap = getDataGanttColumn("MAP", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);

    System.out.println(actualMapMessage);
    System.out.println(actualMessageMap);
    System.out.println(actualMapMap);
  }

  @Test
  public void computeRawTest() {
    List<List<Object>> expected01 = transpose(data01);
    List<List<Object>> expected02 = transpose(data02);
    List<List<Object>> expected03 = transpose(data03);
    expected01.addAll(expected02);
    expected01.addAll(expected03);

    List<List<Object>> actual = getRawDataAll(Integer.MIN_VALUE, Integer.MAX_VALUE);

    System.out.println(actual);

    assertEquals(String.valueOf(expected01), String.valueOf(actual));
  }
}
