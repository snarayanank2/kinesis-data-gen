package com.qubole.kinesis;

import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class RecordReaderTest {
  private final static Logger LOGGER = Logger.getLogger(RecordReaderTest.class
      .getName());

  @Test
  public void testRecordParsing() {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream("/sample_data.txt");
    RecordReader rr = new RecordReader(is);
    int records = 0;
    while (rr.hasNext()) {
      Record rec = rr.next();
      Assert.assertNotNull(rec);
      Assert.assertNotNull(rec.getTimestamp());
      Assert.assertNotNull(rec.getHost());
      Assert.assertNotNull(rec.getRequest());
      records++;
      LOGGER.log(Level.INFO, "record " + rec);
    }
    Assert.assertEquals("unexpected number of records", 7, records);
    Assert.assertEquals("record counting is off", 7, rr.getNumRecords());
    Assert.assertEquals("skipped record counting is off", 1, rr.getSkippedRecords());
  }
  
  @Test
  public void testProducerConsumer() throws InterruptedException, ExecutionException, TimeoutException {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream("/sample_data.txt");
    RecordReader rr = new RecordReader(is);
    ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));
    ArrayBlockingQueue<Record> queue = Queues.newArrayBlockingQueue(10000);
    RecordProducer producer = new RecordProducer(rr, queue);
    final DiscardingRecordConsumer consumer = new DiscardingRecordConsumer(queue);
    ListenableFuture<?> consumerFuture = service.submit(consumer);
    ListenableFuture<?> producerFuture = service.submit(producer);
    producerFuture.addListener(new Runnable() {
      @Override
      public void run() {
        consumer.setProducerDone();
      }}, service);
    
    List<?> reponses = Futures.allAsList(producerFuture, consumerFuture).get(20, TimeUnit.SECONDS);
    service.shutdownNow();
    boolean success = service.awaitTermination(10, TimeUnit.SECONDS);
    Assert.assertEquals(true, success);
    Assert.assertEquals("unexpected number of rows from consumer", 7, consumer.getNumRecords());
  }

}
