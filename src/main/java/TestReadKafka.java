import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
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
                .option("kafka.bootstrap.servers", "192.168.1.70:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
        StructType orderSchema = new StructType().add("name", "string").add("age", "int").add("timestamp", "timestamp");

        Dataset<Row> orderDF = df
                .select(from_json(col("value").cast("string"), orderSchema).alias("parsed_value"))
                .select("parsed_value.*");
        Dataset<Row> windowedCounts = orderDF.
                withWatermark("timestamp","2 minutes").
                groupBy(window(col("timestamp"),"1 minutes"), col("name")
                ).count();

        windowedCounts.printSchema();

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();


        //sink如果是kafka必须有value字段，同时必须指定checkpointLocation
//        StreamingQuery query = windowedCounts
//                .toJSON().as("value")
//                .select(col("value").cast("string"))
//                .writeStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "192.168.1.70:9092")
//                .option("topic", "out")
//                .trigger(Trigger.ProcessingTime("1 minutes"))
//                .outputMode("update")
//                .option("checkpointLocation", "C://tmp//dir")
//                .start();

        query.awaitTermination();

    }
}

