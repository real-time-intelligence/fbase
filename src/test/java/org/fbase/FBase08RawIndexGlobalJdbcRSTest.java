package org.fbase;

import lombok.extern.log4j.Log4j2;
import org.fbase.common.AbstractH2Test;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.SType;
import org.fbase.model.profile.table.IType;
import org.fbase.model.profile.table.TType;
import org.fbase.sql.BatchResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Log4j2
public class FBase08RawIndexGlobalJdbcRSTest extends AbstractH2Test {

  private List<List<Object>> expected = new ArrayList<>();

  @BeforeAll
  public void init() {
    loadExpected(expected);

    Map<String, SType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", SType.RAW);
    csTypeMap.put("FIRSTNAME", SType.RAW);
    csTypeMap.put("LASTNAME", SType.RAW);
    csTypeMap.put("HOUSE", SType.HISTOGRAM);
    csTypeMap.put("CITY", SType.ENUM);

    putDataJdbc(csTypeMap, TType.TIME_SERIES, IType.GLOBAL, true);
  }

  @Test
  public void computeTableRawDataBeginEnd012Test() {
    List<List<Object>> actual = getRawDataAll(0, 12);

    assertEquals(expected.size(), actual.size());
    assertForRaw(expected, actual);
  }

  @Test
  public void computeTableRawDataBeginEnd77Test() {
    List<List<Object>> actual = getRawDataAll(7, 7);

    assertEquals(expected.stream().filter(e -> e.get(0) == "7").count(), actual.size());
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()),  RSTest(7, 7, 1));
  }

  @Test
  public void computeTableRawDataBeginEnd57Test() {
    List<List<Object>> actual = getRawDataAll(5, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "5" | e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  RSTest(5, 7, 1));
  }

  @Test
  public void computeTableRawDataBeginEnd67Test() {
    List<List<Object>> actual = getRawDataAll(6, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  RSTest(6, 7, 1));
  }

  @Test
  public void computeTableRawDataBeginEnd16Test() {
    List<List<Object>> actual = getRawDataAll(1, 6);

    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3"
        | e.get(0) == "4" | e.get(0) == "5" | e.get(0) == "6");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  RSTest(1, 6, 1));
  }

  @Test
  public void computeTableRawDataBeginEnd811Test() {
    List<List<Object>> actual = getRawDataAll(8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()),  RSTest(8, 11, 1));
  }

  @Test
  public void computeTableRawDataByColumnIdBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("ID");

    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(0))).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataByColumnFirstNameBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("FIRSTNAME");
    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(1))).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataByColumnCityBeginEnd811Test() {
    CProfile cProfile = getCProfileByColumnName("CITY");
    List<List<Object>> actual = getRawDataByColumn(cProfile, 8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).map(map -> List.of(map.get(0), map.get(4))).collect(Collectors.toList()), actual);
  }

  private List<List<Object>> RSTest(int begin, int end, int fetchSize) {
    BatchResultSet batchResultSet = fStore.getBatchResultSet(tableName, begin, end, fetchSize);

    List<List<Object>> actual = new ArrayList<>();
    while (batchResultSet.next()) {
      List<List<Object>> var = batchResultSet.getObject();
      log.info("Output by fetchSize: " + var);

      actual.addAll(var);
    }

    return actual;
  }

}
