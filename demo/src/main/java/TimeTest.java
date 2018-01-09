import FlinkConsumer.LogConsumer;
import KafkaProducer.LogProducer;

public class TimeTest {


    public static void main(String[] args) throws Exception {

        String path = "displaypolicyd.log";
        String topic = "kafkaflink.demo.test";


        LogProducer logs = new LogProducer().setBatchRecord(path).setTopic(topic);

        //async
        logs.asyncBatchPush();

        //sync
        logs.syncBatchPush();
    }
}
