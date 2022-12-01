package org.fbase.service.container;

import static org.fbase.core.Mapper.DOUBLE_NULL;
import static org.fbase.core.Mapper.FLOAT_NULL;
import static org.fbase.core.Mapper.INT_NULL;
import static org.fbase.core.Mapper.LONG_NULL;

import lombok.EqualsAndHashCode;
import org.fbase.core.Mapper;
import org.fbase.model.profile.CProfile;
import org.fbase.model.profile.cstype.CType;
import org.fbase.storage.dto.RawDto;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class RawContainer {

  @EqualsAndHashCode.Include
  private final long key;

  private final CProfile cProfile;

  private final RawDto rawDto;

  public RawContainer(long key, CProfile cProfile, RawDto rawDto) {
    this.key = key;
    this.cProfile = cProfile;
    this.rawDto = rawDto;
  }

  public String getStrValueForCell(int iRow) {
    if (CType.INT == Mapper.isCType(cProfile)) {
      return String.valueOf(rawDto.getDataInt()[iRow] == INT_NULL ? "" : rawDto.getDataInt()[iRow]);
    } else if (CType.LONG == Mapper.isCType(cProfile)) {
      return String.valueOf(rawDto.getDataLong()[iRow] == LONG_NULL ? "" : rawDto.getDataLong()[iRow]);
    } else if (CType.FLOAT == Mapper.isCType(cProfile)) {
      return String.valueOf(rawDto.getDataFloat()[iRow] == FLOAT_NULL ? "" : rawDto.getDataFloat()[iRow]);
    } else if (CType.DOUBLE == Mapper.isCType(cProfile)) {
      return String.valueOf(rawDto.getDataDouble()[iRow] == DOUBLE_NULL ? "" : rawDto.getDataDouble()[iRow]);
    } else if (CType.STRING == Mapper.isCType(cProfile)) {
      return rawDto.getDataString()[iRow] == null ? "" : rawDto.getDataString()[iRow];
    } else {
      return "";
    }
  }

  public byte getEnumValueForCell(int iRow) {
    return rawDto.getDataByte()[iRow];
  }
}
