import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class TestReadKafka {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.31.54:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .load();
        StructType orderSchema = new StructType().add("name", "string").add("age", "int").add("timestamp", "timestamp");

        Dataset<Row> orderDF = df
                .select(functions.from_json(functions.col("value").cast("string"), orderSchema).alias("parsed_value"))
                .select("parsed_value.*");
        //每隔一分钟计算一分钟之前的 2分钟时间段内的数据
        Dataset<Row> windowedCounts = orderDF.
                withWatermark("timestamp","30 minutes").
                groupBy(functions.window(orderDF.col("timestamp"),"1 minutes"),
                orderDF.col("name")
                ).count();

        windowedCounts.printSchema();

//        StreamingQuery query = windowedCounts.writeStream()
//                .outputMode("update")
//                .format("console")
//                .trigger(Trigger.ProcessingTime("1 minutes"))
//                .start();


        StreamingQuery query = windowedCounts
                .toJSON().as("value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.31.54:9092")
                .option("topic", "out")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .outputMode("update")
                .option("checkpointLocation", "C://tmp//dir")
                .start();


        query.awaitTermination();


    }
}

