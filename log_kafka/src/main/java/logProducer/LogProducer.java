package logProducer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.File;
import java.util.Properties;
import java.util.List;

public class LogProducer {

    private List<LogFile> record;
    private Properties props;
    private String topic;


    public LogProducer() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*
        props.put("key.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("client.id", "ProducerExample");
    }

    @SuppressWarnings("carefully use this method")
    public LogProducer setRecord(List<LogFile> logs) {
        //SHALLOW COPY, TODO
        record = logs;

        return this;
    }

    public LogProducer setRecord(String Path) {
        record = FileScan.scanByFileName(Path);

        return this;
    }

    public LogProducer setRecord(File Path) {
        record = FileScan.scanByFileName(Path);

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

    public void pushLog() {
        Producer<String, LogFile> producer = new KafkaProducer<>(props);
        System.out.println("Generated log id is" + record.toString());
        try {
            for (LogFile eachLog : record) {
                //async
                ProducerRecord<String, LogFile> logfile = new ProducerRecord<>(topic, eachLog.getLogId(), eachLog);
                producer.send(logfile, (recordMetadata, e) -> {});

                //sync
                ProducerRecord<String, LogFile> logfileCopy = new ProducerRecord<>(topic, eachLog.getLogId(), eachLog);
                producer.send(logfileCopy).get();
            }
        } catch (Exception ex) {
            System.out.println("oops");
            ex.printStackTrace();
        }
    }
}