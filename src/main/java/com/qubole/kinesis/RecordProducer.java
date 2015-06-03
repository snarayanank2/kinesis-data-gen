package com.qubole.kinesis;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RecordProducer implements Runnable {
  private final static Logger LOGGER = Logger.getLogger(RecordProducer.class
      .getName());

  private RecordReader rr;
  private ArrayBlockingQueue<Record> queue;
  public RecordProducer(RecordReader rr, ArrayBlockingQueue<Record> queue) {
    this.rr = rr;
    this.queue = queue;
  }

  @Override
  public void run() {
    while (rr.hasNext()) {
      Record rec = rr.next();
      try {
        if (rec != null) {
          queue.put(rec);
        }
      } catch (InterruptedException e) {
        LOGGER.log(Level.INFO, "producer is interrupted. stopping.");
        break;
      }
    }
    LOGGER.log(Level.INFO, "producer is done");
  }
}
