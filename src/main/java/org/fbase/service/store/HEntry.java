package org.fbase.service.store;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HEntry {
  List<Integer> index;
  List<Integer> value;
}
