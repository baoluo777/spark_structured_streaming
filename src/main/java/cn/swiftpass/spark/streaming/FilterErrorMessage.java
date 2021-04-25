package cn.swiftpass.spark.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class FilterErrorMessage {
    private static final String kafkaIp = "127.0.0.1:9092";

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("monitorOrderSuccessRate")
                .getOrCreate();
        //配置kafka作为source
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
        //直接处理每一条数据，和storm一样，用来做一些监控告警规则。
        Dataset<Row> ss = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        Dataset errors = ss.filter(ss.col("value").like("%ERROR%"));
        write2console(errors);
        spark.streams().awaitAnyTermination();
    }

    private static void write2console(Dataset<Row> countDf) throws TimeoutException {
        countDf.writeStream().outputMode("append").format("console").start();
    }
}
