package org.fbase.model.histogram;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class IVEntry {
  List<Integer> index;
  List<Integer> value;
}
