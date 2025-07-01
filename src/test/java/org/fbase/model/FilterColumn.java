package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class FilterColumn {
  private String columnName;
  private String columnNameFilter;
  private String filterValue;
}
