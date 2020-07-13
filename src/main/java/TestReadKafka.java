import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
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
//                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
        StructType orderSchema = new StructType()
                .add("id", "string")
                .add("free", "string")
                .add("state", "string")
                .add("bank_no", "string")
                .add("agent_id", "string")
                .add("merchant_id", "string")
                .add("trade_time", "timestamp");

        Dataset<Row> orderDF = df
                .select(from_json(col("value").cast("string"), orderSchema).alias("parsed_value"))
                .select("parsed_value.*");

//        method1(orderDF);
//        method2(orderDF);
//        method3(orderDF);
        method4(orderDF,spark);

        spark.streams().awaitAnyTermination();

    }

    /**
     * sink is console demo
     * @param orderDF
     * @throws TimeoutException
     */
    private static void method1(Dataset<Row> orderDF) throws TimeoutException {
        Dataset<Row> windowedCounts = orderDF.
                withWatermark("trade_time","1 minutes").
                groupBy(window(col("trade_time"),"1 minutes"), col("bank_no")
                ).count();

        windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();
    }

    /**
     * kafka sink demo
     * sink如果是kafka必须有value字段，同时必须指定checkpointLocation
     * @param orderDF
     * @throws TimeoutException
     */
    private static void method2(Dataset<Row> orderDF) throws TimeoutException {
        Dataset<Row> windowedCounts = orderDF.
                withWatermark("trade_time","1 minutes").
                groupBy(window(col("trade_time"),"1 minutes"), col("bank_no")
                ).count();
       windowedCounts
                .toJSON().as("value")
                .select(col("value").cast("string"))
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.1.70:9092")
                .option("topic", "out")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .outputMode("update")
                .option("checkpointLocation", "C://tmp//dir")
                .start();
    }

    /**
     * without merchant id filter
     * @param orderDF
     * @throws TimeoutException
     */
    private static void method3(Dataset<Row> orderDF) throws TimeoutException {
        Dataset<Row> windowedCounts = orderDF.
                withWatermark("trade_time","1 minutes").
                groupBy(window(col("trade_time"),"1 minutes"), col("merchant_id")
                ).count();
        windowedCounts.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();
    }
    /**
     * with merchant id filter
     * @param orderDF
     * @param ss
     * @throws TimeoutException
     */
    private static void method4(Dataset<Row> orderDF, SparkSession ss) throws TimeoutException {
        Dataset<Row> fiterMerchantIdDf = ss
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://192.168.1.70:3306/monitor?characterEncoding=utf8&useSSL=false")
                .option("dbtable", "filter_merchant_ids")
                .option("user", "root")
                .option("password", "bigdata@2019")
                .load();

        Dataset<Row>   afterFilterMerchantIdDf= orderDF.join(fiterMerchantIdDf,"merchant_id");

        Dataset<Row> windowedCounts = afterFilterMerchantIdDf.
                withWatermark("trade_time","1 minutes").
                groupBy(window(col("trade_time"),"1 minutes"), col("merchant_id"),col("state")
                ).count();
//Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode
        windowedCounts.writeStream()
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();
    }
}

