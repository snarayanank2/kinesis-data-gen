package com.qubole.kinesis.core;

public interface StreamProducer<T> {
  public void start();
  public void end();
  public long records();
  public T next();
}
