package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class BeginAndEnd<B, E> {
  private B begin;
  private E end;
}
