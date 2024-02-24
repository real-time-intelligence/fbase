package org.fbase;

import static org.fbase.service.CommonServiceApi.transpose;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.fbase.common.AbstractDirectTest;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.metadata.DataType;
import org.fbase.model.GroupFunction;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.SProfile;
import org.fbase.model.profile.cstype.CSType;
import org.fbase.model.profile.cstype.CType;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.BType;
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
    sProfile.setBackendType(BType.BERKLEYDB);
    sProfile.setCompression(true);

    Map<String, CSType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", CSType.builder().isTimeStamp(true).sType(SType.RAW).cType(CType.LONG).dType(DataType.LONG).build());
    csTypeMap.put("MESSAGE", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.VARCHAR).build());
    csTypeMap.put("MAP", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.MAP).build());
    csTypeMap.put("ARRAY", CSType.builder().isTimeStamp(false).sType(SType.RAW).cType(CType.STRING).dType(DataType.ARRAY).build());

    sProfile.setCsTypeMap(csTypeMap);

    putDataDirect(sProfile);
  }

  @Test
  public void computeStackedTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listMessage = getDataStackedColumn("MESSAGE", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<StackedColumn> listMap = getDataStackedColumn("MAP", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<StackedColumn> listArray = getDataStackedColumn("ARRAY", GroupFunction.COUNT, Integer.MIN_VALUE, Integer.MAX_VALUE);

    assertEquals(findListStackedKey(listMessage, testMessage1), testMessage1);
    assertEquals(findListStackedValue(listMessage, testMessage1), 2);

    assertEquals(findListStackedKey(listMessage, testMessage2), testMessage2);
    assertEquals(findListStackedValue(listMessage, testMessage2), 2);

    compareKeySetForMapDataType(testMap1, listMap);
    compareKeySetForMapDataType(testMap2, listMap);
    assertEquals(testMap3, listMap.stream().filter(f -> f.getKeyCount().isEmpty()).findAny().orElseThrow().getKeyCount());

    assertEquals(getTestDataArray(), listArray);
  }

  @Test
  public void computeGanttTest() throws BeginEndWrongOrderException, SqlColMetadataException, GanttColumnNotSupportedException {
    List<GanttColumn> actualMapMessage = getDataGanttColumn("MAP", "MESSAGE", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualArrayMessage = getDataGanttColumn("ARRAY", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    assertEquals(getTestDataMapMessage(), actualMapMessage);
    assertEquals(getTestArrayMessage(), actualArrayMessage);

    List<GanttColumn> actualMessageMap = getDataGanttColumn("MESSAGE", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualMapMap = getDataGanttColumn("MAP", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualArrayMap = getDataGanttColumn("ARRAY", "MAP", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualMapArray = getDataGanttColumn("MAP", "ARRAY", Integer.MIN_VALUE, Integer.MAX_VALUE);
    List<GanttColumn> actualArrayArray = getDataGanttColumn("ARRAY", "ARRAY", Integer.MIN_VALUE, Integer.MAX_VALUE);

    System.out.println(actualMessageMap);
    System.out.println(actualMapMap);
    System.out.println(actualArrayMap);
    System.out.println(actualMapArray);
    System.out.println(actualArrayArray);
  }

  @Test
  public void computeRawTest() {
    List<List<Object>> expected01 = transpose(data01);
    List<List<Object>> expected02 = transpose(data02);
    List<List<Object>> expected03 = transpose(data03);
    expected01.addAll(expected02);
    expected01.addAll(expected03);

    List<List<Object>> actual = getRawDataAll(Integer.MIN_VALUE, Integer.MAX_VALUE);

    String expected = expected01.stream()
        .map(innerList -> innerList.stream()
            .map(obj -> obj instanceof String[] ?
                Arrays.toString((String[]) obj) :
                obj instanceof Object[] ?
                    Arrays.deepToString((Object[]) obj) :
                    obj.toString())
            .collect(Collectors.joining(", ")))
        .collect(Collectors.joining("], [", "[", "]"));

    assertEquals("[" + expected + "]", String.valueOf(actual));
  }

  private List<StackedColumn> getTestDataArray() {
    List<StackedColumn> expected = new ArrayList<>();

    expected.add(new StackedColumn(0, 1, createKeyCountMap("array value 1=2", "array value 2=2"),
                                   Collections.emptyMap(), Collections.emptyMap()));
    expected.add(new StackedColumn(10, 11, createKeyCountMap("array value 1=2", "array value 2=2"),
                                   Collections.emptyMap(), Collections.emptyMap()));
    expected.add(new StackedColumn(20, 21, createKeyCountMap("array value 1=2", "array value 2=2"),
                                   Collections.emptyMap(), Collections.emptyMap()));

    return expected;
  }

  private List<GanttColumn> getTestDataMapMessage() {
    List<GanttColumn> expected = new ArrayList<>();
    Map<String, Integer> ganttData;

    ganttData = new HashMap<>();
    ganttData.put("Test message 3", 2);
    expected.add(new GanttColumn("", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 12);
    expected.add(new GanttColumn("val6", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 10);
    expected.add(new GanttColumn("val5", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 2", 8);
    expected.add(new GanttColumn("val4", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 6);
    expected.add(new GanttColumn("val3", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 4);
    expected.add(new GanttColumn("val2", ganttData));

    ganttData = new HashMap<>();
    ganttData.put("Test message 1", 2);
    expected.add(new GanttColumn("val1", ganttData));

    return expected;
  }

  private List<GanttColumn> getTestArrayMessage() {
    List<GanttColumn> expected = new ArrayList<>();

    expected.add(new GanttColumn("array value 1", createGanttMap("=2", "val6=12", "val5=10", "val4=8", "val3=6" , "val2=4", "val1=2")));
    expected.add(new GanttColumn("array value 2", createGanttMap("=2", "val6=12", "val5=10", "val4=8", "val3=6" , "val2=4", "val1=2")));

    return expected;
  }

  private Map<String, Integer> createKeyCountMap(String... entries) {
    Map<String, Integer> map = new HashMap<>();
    for (String entry : entries) {
      String[] parts = entry.split("=");
      map.put(parts[0], Integer.parseInt(parts[1]));
    }
    return map;
  }

  private Map<String, Integer> createGanttMap(String... entries) {
    Map<String, Integer> map = new HashMap<>();
    for (String entry : entries) {
      if(!entry.isEmpty()) {
        String[] parts = entry.split("=");
        map.put(parts[0], Integer.parseInt(parts[1]));
      }
    }
    return map;
  }
}
