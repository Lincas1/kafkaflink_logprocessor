package dataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataLocalTransformation {


    public static void main(String[] args) {

        System.out.println("123");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> integers = env.fromElements(1,2,3,4);

        DataStream<Integer> doubleIntegers = integers.map(new MapFunction<Integer, Integer>() {

            public Integer map(Integer values) throws Exception {
                return 2 * values;
            }
        });

        doubleIntegers.print();
    }
}
