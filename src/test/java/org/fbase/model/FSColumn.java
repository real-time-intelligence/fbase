package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString.Exclude;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class FSColumn {
  private String firstCol;
  private String secondCol;
}
