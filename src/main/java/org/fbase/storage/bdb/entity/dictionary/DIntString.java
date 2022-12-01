package org.fbase.storage.bdb.entity.dictionary;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class DIntString {

  @PrimaryKey(sequence = "ParamDIntStringSeq")
  private int param;

  @SecondaryKey(relate = Relationship.ONE_TO_ONE)
  private String value;
}
