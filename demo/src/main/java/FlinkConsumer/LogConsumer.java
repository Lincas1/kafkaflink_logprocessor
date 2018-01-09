package FlinkConsumer;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;


public class LogConsumer {

    private Properties props;

    private String topic;

    private DeserializationSchema<String> deSchema;

    private DataStream<String> stream;

    private final StreamExecutionEnvironment env;

    public LogConsumer() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DEFAULT properties
        props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        props.setProperty("zookeeper.connect", "localhost:2181");
        props.setProperty("group.id", "test");

        //Default deserializationSchema
        deSchema = new SimpleStringSchema();
    }

    public void pullLog() {
        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>(topic, deSchema, props);
        myConsumer.setStartFromEarliest();
        //myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

        stream = env.addSource(myConsumer);
    }

    public LogConsumer setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public LogConsumer setProps(Properties props) {
        this.props = props;
        return this;
    }

    public LogConsumer setDeSchema(DeserializationSchema<String> deSchema) {
        this.deSchema = deSchema;
        return this;
    }

    public DataStream<String> getStream() {
        return stream;
    }

    public void batchPullLog() throws Exception {
        //need to add exception

        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>(topic, deSchema, props);

        myConsumer.setStartFromEarliest();
        //myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

        stream = env.addSource(myConsumer);

        stream.map((value) -> {return "Stream Value : " + value; });

        stream.print();

        env.execute();
    }

}


/*
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class LogConsumer {

    private Properties props;

    private String topic;

    private DeserializationSchema<String> deSchema;

    private DataStream<String> stream;

    private final StreamExecutionEnvironment env;

    public LogConsumer() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DEFAULT properties
        props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        props.setProperty("zookeeper.connect", "localhost:2181");
        props.setProperty("group.id", "test");

        //Default deserializationSchema
        deSchema = new SimpleStringSchema();
    }

    public void pullLog() {
        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>(topic, deSchema, props);
        myConsumer.setStartFromEarliest();
        //myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

        stream = env.addSource(myConsumer);
    }

    public LogConsumer setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public LogConsumer setProps(Properties props) {
        this.props = props;
        return this;
    }

    public LogConsumer setDeSchema(DeserializationSchema<String> deSchema) {
        this.deSchema = deSchema;
        return this;
    }

    public DataStream<String> getStream() {
        return stream;
    }


    public static void main(String[] args) throws Exception {
        //need to add exception
        LogConsumer my = new LogConsumer().setTopic("testtopic1");
        System.out.println(1);
        my.pullLog();
        DataStream<String> stream = my.getStream();
        stream.map((value) -> {return "Stream Value : " + value; });
        my.getStream().print();

        my.env.execute();
    }
}
*/
