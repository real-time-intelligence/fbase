package org.fbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.fbase.common.AbstractH2Test;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.StackedColumn;
import org.fbase.model.profile.cstype.SType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FBase05EnumTest extends AbstractH2Test {
  
  private List<List<Object>> expected = new ArrayList<>();

  @BeforeAll
  public void init() {
    loadExpected(expected);

    Map<String, SType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", SType.RAW);
    csTypeMap.put("LASTNAME", SType.ENUM);
    csTypeMap.put("HOUSE", SType.ENUM);
    csTypeMap.put("CITY", SType.ENUM);

    putData(csTypeMap);
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
    assertForRaw(expected.stream().filter(e -> e.get(0) == "7").collect(Collectors.toList()),
        actual);
  }

  @Test
  public void computeTableRawDataBeginEnd57Test() {
    List<List<Object>> actual = getRawDataAll(5, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "5" | e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd67Test() {
    List<List<Object>> actual = getRawDataAll(6, 7);

    Predicate<List<Object>> filter = e -> (e.get(0) == "6" | e.get(0) == "7");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd16Test() {
    List<List<Object>> actual = getRawDataAll(1, 6);

    Predicate<List<Object>> filter = e -> (e.get(0) == "1" | e.get(0) == "2" | e.get(0) == "3"
        | e.get(0) == "4" | e.get(0) == "5" | e.get(0) == "6");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeTableRawDataBeginEnd811Test() {
    List<List<Object>> actual = getRawDataAll(8, 11);

    Predicate<List<Object>> filter = e -> (e.get(0) == "8" | e.get(0) == "9"
        | e.get(0) == "10" | e.get(0) == "11");

    assertEquals(expected.stream().filter(filter).count(), actual.size());
    assertForRaw(expected.stream().filter(filter).collect(Collectors.toList()), actual);
  }

  @Test
  public void computeBeginEndStackedColumnTest() throws BeginEndWrongOrderException, SqlColMetadataException {
    List<StackedColumn> listIndexed12 = getStackedData("LASTNAME", 1, 2);
    List<StackedColumn> listNotIndexed12 = getStackedData("FIRSTNAME", 1, 2);

    assertEquals(firstListStackedKey(listIndexed12), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed12), 2);
    assertEquals(firstListStackedKey(listNotIndexed12), "Alex");
    assertEquals(firstListStackedValue(listNotIndexed12), 1);
    assertEquals(lastListStackedKey(listIndexed12), "Ivanov");
    assertEquals(lastListStackedKey(listNotIndexed12), "Ivan");

    List<StackedColumn> listIndexed23 = getStackedData("LASTNAME", 2, 3);
    List<StackedColumn> listNotIndexed23 = getStackedData("FIRSTNAME", 2, 3);

    assertEquals(firstListStackedKey(listIndexed23), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed23), 1);
    assertEquals(firstListStackedKey(listNotIndexed23), "Ivan");
    assertEquals(firstListStackedValue(listNotIndexed23), 1);
    assertEquals(lastListStackedKey(listIndexed23), "Petrov");
    assertEquals(lastListStackedKey(listNotIndexed23), "Oleg");

    List<StackedColumn> listIndexed34 = getStackedData("LASTNAME", 3, 4);
    List<StackedColumn> listNotIndexed34 = getStackedData("FIRSTNAME", 3, 4);

    assertEquals(firstListStackedKey(listIndexed34), "Petrov");
    assertEquals(firstListStackedValue(listIndexed34), 1);
    assertEquals(firstListStackedKey(listNotIndexed34), "Oleg");
    assertEquals(firstListStackedValue(listNotIndexed34), 1);
    assertEquals(lastListStackedKey(listIndexed34), "Sui");
    assertEquals(lastListStackedKey(listNotIndexed34), "Lee");

    List<StackedColumn> listIndexed79 = getStackedData("LASTNAME", 7, 9);
    List<StackedColumn> listNotIndexed79 = getStackedData("FIRSTNAME", 7, 9);

    assertEquals(firstListStackedKey(listIndexed79), "Petrov");
    assertEquals(firstListStackedValue(listIndexed79), 1);
    assertEquals(firstListStackedKey(listNotIndexed79), "Men");
    assertEquals(firstListStackedValue(listNotIndexed79), 1);
    assertEquals(lastListStackedKey(listIndexed79), "Шаляпин");
    assertEquals(lastListStackedKey(listNotIndexed79), "Федор");

    List<StackedColumn> listIndexed59 = getStackedData("LASTNAME", 5, 9);
    List<StackedColumn> listNotIndexed59 = getStackedData("FIRSTNAME", 5, 9);

    assertEquals(firstListStackedKey(listIndexed59), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed59), 2);
    assertEquals(firstListStackedKey(listNotIndexed59), "Lee");
    assertEquals(firstListStackedValue(listNotIndexed59), 2);
    assertEquals(lastListStackedKey(listIndexed59), "Шаляпин");
    assertEquals(lastListStackedKey(listNotIndexed59), "Федор");

    List<StackedColumn> listIndexed010 = getStackedData("LASTNAME", 0, 10);
    List<StackedColumn> listNotIndexed010 = getStackedData("FIRSTNAME", 0, 10);

    assertEquals(firstListStackedKey(listIndexed010), "Ivanov");
    assertEquals(firstListStackedValue(listIndexed010), 4);
    assertEquals(firstListStackedKey(listNotIndexed010), "Alex");
    assertEquals(firstListStackedValue(listNotIndexed010), 1);
    assertEquals(lastListStackedKey(listIndexed010), "Пирогов");
    assertEquals(lastListStackedKey(listNotIndexed010), "Петр");

    List<StackedColumn> listIndexed1111 = getStackedData("LASTNAME", 11, 11);
    List<StackedColumn> listNotIndexed1111 = getStackedData("FIRSTNAME", 11, 11);

    assertEquals(firstListStackedKey(listIndexed1111), "Semenov");
    assertEquals(firstListStackedValue(listIndexed1111), 1);
    assertEquals(firstListStackedKey(listNotIndexed1111), "Oleg");
    assertEquals(firstListStackedValue(listNotIndexed1111), 1);
  }

}
