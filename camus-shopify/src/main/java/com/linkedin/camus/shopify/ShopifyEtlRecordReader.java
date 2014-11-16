package com.linkedin.camus.shopify;

import com.linkedin.camus.etl.kafka.mapred.EtlRecordReader;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.io.IOException;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ShopifyEtlRecordReader extends EtlRecordReader {
    public static final String STATSD_ENABLED = "statsd.enabled";
    public static final String STATSD_HOST = "statsd.host";
    public static final String STATSD_PORT = "statsd.port";

    private static boolean statsdEnabled;
    private static StatsDClient statsd;


    public ShopifyEtlRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        super(split, context);
        this.statsdEnabled = getStatsdEnabled(context);

        if (this.statsdEnabled) {
            this.statsd = new NonBlockingStatsDClient(
                    "Camus",
                    getStatsdHost(context),
                    getStatsdPort(context),
                    new String[]{"camus:exceptions"}
            );
        }
    }

    private CamusWrapper getWrappedRecord(String topicName, byte[] payload) throws IOException {
        CamusWrapper r = null;
        try {
            r = decoder.decode(payload);
        } catch (Exception e) {
            if (this.statsdEnabled) {
                statsd.incrementCounter(topicName.concat(":" + e.getMessage()));
            }
            if (!skipSchemaErrors) {
                throw new IOException(e);
            }
        }
        return r;
    }

    public static Boolean getStatsdEnabled(JobContext job) {
        return job.getConfiguration().getBoolean(STATSD_ENABLED, false);
    }

    public static String getStatsdHost(JobContext job) {
        return job.getConfiguration().get(STATSD_HOST, "localhost");
    }

    public static int getStatsdPort(JobContext job) {
        return job.getConfiguration().getInt(STATSD_PORT, 8125);
    }
}
