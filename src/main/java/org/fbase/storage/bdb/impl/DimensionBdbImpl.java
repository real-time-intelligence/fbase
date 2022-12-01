package org.fbase.storage.bdb.impl;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import lombok.extern.log4j.Log4j2;
import org.fbase.storage.DimensionDAO;
import org.fbase.storage.bdb.entity.dictionary.DIntDouble;
import org.fbase.storage.bdb.entity.dictionary.DIntString;

@Log4j2
public class DimensionBdbImpl implements DimensionDAO {

  public SecondaryIndex<Double, Integer, DIntDouble> secondaryIndexDouble;
  public SecondaryIndex<String, Integer, DIntString> secondaryIndexString;
  private EntityStore store;
  private PrimaryIndex<Integer, DIntDouble> primaryIndexDouble;
  private PrimaryIndex<Integer, DIntString> primaryIndexString;

  public DimensionBdbImpl(EntityStore store) {
    this.store = store;
    this.primaryIndexDouble = store.getPrimaryIndex(Integer.class, DIntDouble.class);
    this.secondaryIndexDouble = store.getSecondaryIndex(primaryIndexDouble, Double.class, "value");

    this.primaryIndexString = store.getPrimaryIndex(Integer.class, DIntString.class);
    this.secondaryIndexString = store.getSecondaryIndex(primaryIndexString, String.class, "value");
  }

  @Override
  public int getOrLoad(double value) {
    if (!secondaryIndexDouble.contains(value)) {
      this.primaryIndexDouble.putNoReturn(new DIntDouble(0, value));
    }
    return this.secondaryIndexDouble.get(value).getParam();
  }

  @Override
  public int getOrLoad(String value) {
    if (!secondaryIndexString.contains(value)) {
      this.primaryIndexString.putNoReturn(new DIntString(0, value));
    }
    return this.secondaryIndexString.get(value).getParam();
  }

  @Override
  public String getStringById(int key) {
    return this.primaryIndexString.get(key).getValue();
  }

  @Override
  public double getDoubleById(int key) {
    return this.primaryIndexDouble.get(key).getValue();
  }
}
