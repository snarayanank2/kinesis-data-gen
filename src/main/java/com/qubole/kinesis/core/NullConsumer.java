package com.qubole.kinesis.core;

public class NullConsumer<T> implements StreamConsumer<T> {
  private long records = 0;

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void end() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void process(T record) {
    records++;
  }

  @Override
  public long records() {
    return records;
  }

}
