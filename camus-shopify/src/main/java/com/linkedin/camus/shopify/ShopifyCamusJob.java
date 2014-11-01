package com.linkedin.camus.shopify;

import com.linkedin.camus.etl.kafka.CamusJob;

public class ShopifyCamusJob extends CamusJob {
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
    super(job, timingMap);
    submitCountersToStatsd(job);
  }

  private void submitCountersToStatsd(Job job) throws IOException {
    Counters counters = job.getCounters();
    if (EtlRecordReader.getStatsdEnabled(job)) {
      StatsDClient statsd = new NonBlockingStatsDClient(
        "Camus",
        EtlRecordReader.getStatsdHost(job),
        EtlRecordReader.getStatsdPort(job),
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
