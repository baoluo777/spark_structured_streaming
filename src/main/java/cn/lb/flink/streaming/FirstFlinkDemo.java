package cn.lb.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;


import java.util.Properties;

public class FirstFlinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        consumeKafka(env);
    }

    private static void consumeKafka(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Object> ss = stream.map(x -> x.contains("error"));
        ss.addSink(new PrintSinkFunction());
        env.execute("Flink-Kafka demo");
    }
//    private static void consumeKafkaWithJSON(StreamExecutionEnvironment env) throws Exception {
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "test");
//        DataStreamSource<ObjectNode> kafkaStream = env
//                .addSource(new FlinkKafkaConsumer<>("test", new JSONKeyValueDeserializationSchema(true), properties));
//        SingleOutputStreamOperator<String> ssss = kafkaStream.process(new ProcessFunction<ObjectNode, String>() {
//            @Override
//            public void processElement(ObjectNode value, Context ctx, Collector<String> out)  {
//                JsonNode name = value.get("value").get("name");
//                System.out.println(name);
//            });
//        }

}
