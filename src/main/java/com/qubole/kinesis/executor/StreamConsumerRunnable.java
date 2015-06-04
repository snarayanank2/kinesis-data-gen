package com.qubole.kinesis.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.qubole.kinesis.core.StreamConsumer;

public class StreamConsumerRunnable<T> implements Runnable {
  private final static Logger LOGGER = Logger
      .getLogger(StreamConsumerRunnable.class.getName());

  private ArrayBlockingQueue<T> queue;
  private boolean producerDone = false;
  private StreamConsumer<T> consumer;
  private long noOpCounter = 0;

  public StreamConsumerRunnable(StreamConsumer<T> consumer, ArrayBlockingQueue<T> queue) {
    this.consumer = consumer;
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
    consumer.start();
    while (true) {
      try {
        // LOGGER.log(Level.INFO, "consumer waiting to take.");
        T rec = queue.poll(100, TimeUnit.MILLISECONDS);
        if (rec != null) {
          consumer.process(rec);
        } else {
          noOpCounter++;
          if (getProducerDone()) {
            break;
          }
        }
      } catch (InterruptedException e) {
        LOGGER.log(Level.INFO, "consumer is interrupted.. ignoring");
      }
    }
    consumer.end();
    LOGGER.log(Level.INFO, "no-op count " + noOpCounter);
  }  
}
