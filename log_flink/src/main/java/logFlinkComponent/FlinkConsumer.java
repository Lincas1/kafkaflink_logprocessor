package logFlinkComponent;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class FlinkConsumer {

    private Properties props;

    private String topic;

    private DeserializationSchema<String> deSchema;

    private DataStream<String> stream;

    private final StreamExecutionEnvironment env;

    public FlinkConsumer() {

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

    public FlinkConsumer setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public FlinkConsumer setProps(Properties props) {
        this.props = props;
        return this;
    }

    public FlinkConsumer setDeSchema(DeserializationSchema<String> deSchema) {
        this.deSchema = deSchema;
        return this;
    }

    public DataStream<String> getStream() {
        return stream;
    }


    public static void main(String[] args) throws Exception {
        //need to add exception
        FlinkConsumer my = new FlinkConsumer().setTopic("test");
        System.out.println(1);
        my.pullLog();
        DataStream<String> stream = my.getStream();
        stream.map((value) -> {return "Stream Value : " + value; });
        my.getStream().print();

        my.env.execute();
    }
}
