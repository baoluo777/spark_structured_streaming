package cn.lb.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class FirstFlinkDemo {
    public static void main(String[] args) throws Exception {
//        consumeKafka();

    }

    public static void consumeKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Object> ss = stream.map(x ->x.contains("error"));
        ss.addSink(new PrintSinkFunction());
        env.execute("Flink-Kafka demo");
    }
    public static void consumeJDBC(){

    }
}
