package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.fbase.backend.BerkleyDB;
import org.fbase.core.FStore;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class BdbAndFBase {
  private FStore fStore;
  private BerkleyDB berkleyDB;
}
