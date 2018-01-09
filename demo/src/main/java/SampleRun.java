import FlinkConsumer.LogConsumer;
import KafkaProducer.LogProducer;

public class SampleRun {

    public static void main(String[] args) throws Exception {

        String path = "displaypolicyd.log";
        String topic = "kafkaflink.demo";

        //producer
        LogProducer logs = new LogProducer().setBatchRecord(path).setTopic(topic);
        logs.asyncBatchPush();

        //consumer
        LogConsumer my = new LogConsumer().setTopic(topic);
        my.batchPullLog();

    }
}
