package com.qubole.kinesis;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DiscardingRecordConsumer implements Runnable {
  private final static Logger LOGGER = Logger
      .getLogger(DiscardingRecordConsumer.class.getName());

  private ArrayBlockingQueue<Record> queue;
  private int numRecords = 0;
  private boolean producerDone = false;

  public DiscardingRecordConsumer(ArrayBlockingQueue<Record> queue) {
    this.queue = queue;
  }

  public void setProducerDone() {
    producerDone = true;
  }

  public boolean getProducerDone() {
    return producerDone;
  }

  @Override
  public void run() {
    long t1 = System.nanoTime();
    while (true) {
      try {
        // LOGGER.log(Level.INFO, "consumer waiting to take.");
        Record rec = queue.poll(1, TimeUnit.SECONDS);
        if (rec != null) {
          // LOGGER.log(Level.INFO, "consumer got a record " + rec);
          numRecords++;
        } else {
          if (getProducerDone()) {
            break;
          }
        }
      } catch (InterruptedException e) {
        LOGGER.log(Level.INFO, "consumer is interrupted. stopping.");
        break;
      }
    }
    long t2 = System.nanoTime();
    double seconds = (t2 - t1) / Math.pow(10, 9);
    LOGGER.log(Level.INFO, "Read " + numRecords + " records");
    LOGGER.log(Level.INFO, "Total time " + seconds + " seconds");
    LOGGER.log(Level.INFO, "Records per second " + (numRecords / seconds));
  }
  
  public int getNumRecords() {
    return numRecords;
  }
}
