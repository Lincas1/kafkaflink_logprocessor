package KafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.File;
import java.util.Properties;
import java.util.List;

public class LogProducer {

    private List<KafkaRecord> batchRecord;
    private Properties props;
    private String topic;


    public LogProducer() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put("schema.registry.url", "http://localhost:8081");
        props.put("client.id", "ProducerExample");
    }

    @SuppressWarnings("carefully use this method")
    public LogProducer setBatchRecord(List<KafkaRecord> logs) {
        //SHALLOW COPY, TODO
        batchRecord = logs;

        return this;
    }

    public LogProducer setBatchRecord(String Path) {

        batchRecord = FileScan.scanByFileName(Path);

        return this;
    }

    public LogProducer setBatchRecord(File Path) {

        batchRecord = FileScan.scanByFileName(Path);

        return this;
    }

    public LogProducer setProperties(Properties props) {

        this.props = props;

        return this;
    }

    public LogProducer setTopic(String topic) {

        this.topic = topic;

        return this;
    }

    public void asyncBatchPush() {
        batchPushLog(true);
    }

    public void syncBatchPush() {
        batchPushLog(false);
    }

    private void batchPushLog(boolean isAsync) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //System.out.println("Generated log id is" + record.toString());

        long t = System.currentTimeMillis();
        for (KafkaRecord eachLog : batchRecord) {
            try {

                if (isAsync) {
                    //async
                    ProducerRecord<String, String> logfile = new ProducerRecord<>(topic, eachLog.getKeyAsString(), eachLog.getValueAsString());
                    producer.send(logfile, (recordMetadata, e) -> {
                    });
                } else {
                    //sync
                    ProducerRecord<String, String> logfileCopy = new ProducerRecord<>(topic, eachLog.getKeyAsString(), eachLog.getValueAsString());
                    producer.send(logfileCopy).get();
                }

            } catch (Exception ex) {
                System.out.println("oops");
                ex.printStackTrace();
            }
        }
        System.out.println("sent per second: " + 10 * 1000 / (System.currentTimeMillis() - t));
        producer.close();
    }
}