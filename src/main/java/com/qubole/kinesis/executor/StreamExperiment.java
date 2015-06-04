package com.qubole.kinesis.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.kinesis.core.StreamConsumer;
import com.qubole.kinesis.core.StreamProducer;

public class StreamExperiment<T> {
  private final static Logger LOGGER = Logger.getLogger(StreamExperiment.class
      .getName());
  private ListeningExecutorService service;
  private StreamProducer<T> producer;
  private List<StreamConsumer<T>> consumers;
  private ArrayBlockingQueue<T> queue;

  public StreamExperiment(int workers, StreamProducer<T> producer, StreamConsumer<T> consumer) {
    service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(workers));
    this.producer = producer;
    this.consumers = new ArrayList<StreamConsumer<T>>(10);
    consumers.add(consumer);
    this.queue = Queues.newArrayBlockingQueue(100000);
  }

  public StreamExperiment(int workers, StreamProducer<T> producer, List<StreamConsumer<T>> consumers) {
    service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(workers));
    this.producer = producer;
    this.consumers = consumers;
    this.queue = Queues.newArrayBlockingQueue(10000);
  }

  public void runExperiment() throws InterruptedException, ExecutionException, TimeoutException {
    long t1 = System.nanoTime();
    List<StreamConsumerRunnable<T>> cruns = new ArrayList<StreamConsumerRunnable<T>>(consumers.size());
    for (StreamConsumer<T> consumer : consumers) {
      StreamConsumerRunnable<T> crun = new StreamConsumerRunnable<T>(consumer, queue);
      service.submit(crun);
      cruns.add(crun);
    }

    StreamProducerRunnable<T> prun = new StreamProducerRunnable<T>(producer, queue);
    ListenableFuture<?> pfuture = service.submit(prun);
    pfuture.get();
    LOGGER.log(Level.INFO, "producer is done");
    for (StreamConsumerRunnable<T> crun : cruns) {
      crun.setProducerDone();
    }
    service.shutdown();
    LOGGER.log(Level.INFO, "waiting for consumers to finish");
    service.awaitTermination(200, TimeUnit.SECONDS);
    long t2 = System.nanoTime();
    double seconds = (t2 - t1) / Math.pow(10, 9);
    LOGGER.log(Level.INFO, "Produced " + producer.records() + " records");
    long consumed = 0;
    for (StreamConsumer<T> consumer: consumers) {
      consumed = consumed + consumer.records();
    }
    LOGGER.log(Level.INFO, "Consumed " + consumed + " records");
    LOGGER.log(Level.INFO, "Total time " + seconds + " seconds");
    LOGGER.log(Level.INFO, "Records per second " + (producer.records() / seconds));
  }
}
