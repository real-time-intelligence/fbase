package org.fbase;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fbase.common.AbstractH2Test;
import org.fbase.exception.BeginEndWrongOrderException;
import org.fbase.exception.GanttColumnNotSupportedException;
import org.fbase.exception.SqlColMetadataException;
import org.fbase.model.output.GanttColumn;
import org.fbase.model.profile.cstype.SType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FBase02GanttEnumRawTest extends AbstractH2Test {

  @BeforeAll
  public void init() {
    Map<String, SType> csTypeMap = new HashMap<>();
    csTypeMap.put("ID", SType.RAW);
    csTypeMap.put("LASTNAME", SType.ENUM);
    csTypeMap.put("HOUSE", SType.ENUM);
    csTypeMap.put("CITY", SType.RAW);

    putData(csTypeMap);
  }

  @Test
  public void computeBeginEnd57Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1),
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 5, 7);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEnd67Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 6, 7);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEnd26Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 3)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 2, 6);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEnd16Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 4)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 1, 6);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEnd17Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1),
            entry("Moscow", 4)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 1, 7);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEnd111Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1),
            entry("Moscow", 8)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 1, 11);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndLastNameCity111Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("Ivanov").gantt(Map.ofEntries(
            entry("Moscow", 4)
    )).build());
    expected.add(GanttColumn.builder().key("Petrov").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1),
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Sui").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Тихий").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Шаляпин").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Пирогов").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Semenov").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("LASTNAME", "CITY", 1, 11);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndLastNameHouse1214Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("Mirko").gantt(Map.ofEntries(
            entry("2", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Vedel").gantt(Map.ofEntries(
            entry("3", 1)
    )).build());
    expected.add(GanttColumn.builder().key("Tan").gantt(Map.ofEntries(
            entry("1", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("LASTNAME", "HOUSE", 12, 14);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndCityHouse111Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("Moscow").gantt(Map.ofEntries(
            entry("1", 8),
            entry("2", 2)
    )).build());
    expected.add(GanttColumn.builder().key("Yekaterinburg").gantt(Map.ofEntries(
            entry("1", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("CITY", "HOUSE", 1, 11);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndCityHouse16Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("Moscow").gantt(Map.ofEntries(
            entry("1", 4),
            entry("2", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("CITY", "HOUSE", 1, 6);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndHouseCity16Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 4)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 1, 6);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndHouseCity1214Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumn.builder().key("3").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 12, 14);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndHouseCity1114Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 2)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumn.builder().key("3").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 11, 14);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndHouseCity114Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 9),
            entry("Yekaterinburg", 1)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Yekaterinburg", 1),
            entry("Moscow", 2)
    )).build());
    expected.add(GanttColumn.builder().key("3").gantt(Map.ofEntries(
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 1, 14);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }

  @Test
  public void computeBeginEndHouseCity1525Test()
          throws BeginEndWrongOrderException, GanttColumnNotSupportedException, SqlColMetadataException {
    List<GanttColumn> expected = new ArrayList<>();
    expected.add(GanttColumn.builder().key("1").gantt(Map.ofEntries(
            entry("Moscow", 1),
            entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumn.builder().key("2").gantt(Map.ofEntries(
            entry("Moscow", 1),
            entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumn.builder().key("3").gantt(Map.ofEntries(
            entry("Ufa", 1),
            entry("Yekaterinburg", 2)
    )).build());
    expected.add(GanttColumn.builder().key("4").gantt(Map.ofEntries(
            entry("Ufa", 1),
            entry("Moscow", 1)
    )).build());

    List<GanttColumn> actual =
            getDataGanttColumn("HOUSE", "CITY", 15, 25);

    assertEquals(expected.size(), actual.size());
    assertForGanttColumn(expected, actual);
  }
}