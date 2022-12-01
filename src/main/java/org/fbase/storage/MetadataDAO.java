package org.fbase.storage;

import java.util.List;
import org.fbase.model.profile.CProfile;
import org.fbase.storage.dto.GanttDto;
import org.fbase.storage.dto.MetadataDto;

public interface MetadataDAO {

  boolean put(byte table, long key, byte[] datatype, byte[] function, int[] histograms);

  MetadataDto getMetadata(byte table, long key);

  List<MetadataDto> getListMetadata(byte table, long begin, long end);

  List<GanttDto> getListGanttDto(byte table, long begin, long end, CProfile firstLevelGroupBy, CProfile secondLevelGroupBy);

  long getPreviousKey(byte table, long begin);
  long getLastTimestamp(byte table, long begin, long end);
}
