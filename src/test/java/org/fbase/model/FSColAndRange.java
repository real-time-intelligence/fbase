package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class FSColAndRange <T> {
  private T parameters;
  private BeginAndEnd<Integer, Integer> range;
}
