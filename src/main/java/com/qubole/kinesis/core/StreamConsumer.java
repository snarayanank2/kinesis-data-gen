package com.qubole.kinesis.core;

public interface StreamConsumer<T> {
  public void start();
  public void end();
  public void process(T record);
  public long records();
}
