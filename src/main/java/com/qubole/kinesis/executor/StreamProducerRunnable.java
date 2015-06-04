package com.qubole.kinesis.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.qubole.kinesis.core.StreamProducer;

public class StreamProducerRunnable<T> implements Runnable {
  private final static Logger LOGGER = Logger.getLogger(StreamProducerRunnable.class
      .getName());

  private StreamProducer<T> rr;
  private ArrayBlockingQueue<T> queue;
  public StreamProducerRunnable(StreamProducer<T> rr, ArrayBlockingQueue<T> queue) {
    this.rr = rr;
    this.queue = queue;
  }

  @Override
  public void run() {
    rr.start();
    while (true) {
      T rec = rr.next();
      try {
        if (rec == null) {
          break;
        }
        queue.put(rec);
      } catch (InterruptedException e) {
        LOGGER.log(Level.INFO, "producer is interrupted. stopping.");
        break;
      }
    }
    rr.end();
    LOGGER.log(Level.INFO, "producer is done");
  }
}
