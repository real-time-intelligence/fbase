package org.fbase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.fbase.exception.EnumByteExceedException;
import org.fbase.storage.helper.EnumHelper;
import org.fbase.util.CachedLastLinkedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class FBase07EnumTest {

  @Test
  public void fillMapTest() throws EnumByteExceedException {
    CachedLastLinkedHashMap<Integer, Byte> values = new CachedLastLinkedHashMap<>();

    byte byteValueFirst = EnumHelper.getByteValue(values, 123);
    assertEquals(Byte.MIN_VALUE, byteValueFirst);

    byte byteValueSame = EnumHelper.getByteValue(values, 123);
    assertEquals(Byte.MIN_VALUE, byteValueSame);

    byte byteValueNext = EnumHelper.getByteValue(values, 1234);
    assertEquals(Byte.MIN_VALUE + 1, byteValueNext);
  }

}
