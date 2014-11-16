package com.linkedin.camus.shopify;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import java.util.Map;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.log4j.Logger;

public class ShopifyCamusJob extends CamusJob {

  private final Properties props;
  private static org.apache.log4j.Logger log;

  public ShopifyCamusJob() throws IOException {
    this.props = new Properties();
    this.log = org.apache.log4j.Logger.getLogger(ShopifyCamusJob.class);
  }

  public void runFromAzkaban() throws Exception {
    String camusPropertiesPath = System.getProperty("sun.java.command").split("-P ")[1];
    Properties camusProperties = new Properties();
    InputStream fis = new FileInputStream(camusPropertiesPath);
    camusProperties.load(fis);
    fis.close();
    this.props.putAll(camusProperties);
    run();
  }

  public void createReport(Job job, Map<String, Long> timingMap) throws IOException {
    super.createReport(job, timingMap);
    submitCountersToStatsd(job);
  }

  private void submitCountersToStatsd(Job job) throws IOException {
    Counters counters = job.getCounters();
    if (ShopifyEtlRecordReader.getStatsdEnabled(job)) {
      StatsDClient statsd = new NonBlockingStatsDClient(
        "Camus",
        ShopifyEtlRecordReader.getStatsdHost(job),
        ShopifyEtlRecordReader.getStatsdPort(job),
        new String[]{"camus:counters"}
      );
      for (CounterGroup counterGroup: counters) {
        for (Counter counter: counterGroup){
          statsd.gauge(counterGroup.getDisplayName() + "." + counter.getDisplayName(), counter.getValue());
        }
      }
    }
  }
}
