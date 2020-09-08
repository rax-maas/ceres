package me.itzg.tsdbcassandra.downsample;

public abstract class SingleValueSet extends ValueSet {

  public abstract void setValue(double value);
  public abstract double getValue();

}
