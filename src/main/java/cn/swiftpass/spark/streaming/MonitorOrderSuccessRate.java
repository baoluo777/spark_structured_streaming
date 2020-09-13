package cn.swiftpass.spark.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class MonitorOrderSuccessRate {
    private static String kafkaIp = "192.168.1.70:9092";
    private static String kafkaWriteTopic = "monitorOrder";
    private static String oracleDriver = "oracle.jdbc.OracleDriver";
    private static String oracleUrl = "jdbc:oracle:thin:@192.168.1.225:1521:ndev";
    private static String mysqlDriver = "com.mysql.jdbc.Driver";
    private static String mysqlUrl = "jdbc:mysql://192.168.31.31:3306/monitor";
    private static String userName = "hive";
    private static String password = "hive";


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("monitorOrderSuccessRate")
                .getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("subscribe", "test")
//                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
        StructType orderSchema = new StructType()
                .add("id", "string")
                .add("money", "long")
                .add("state", "string")
                .add("bank_no", "string")
                .add("agent_id", "string")
                .add("merchant_id", "string")
                .add("group_id", "string")
                .add("term_type", "string")
                .add("trade_time", "timestamp");

        Dataset<Row> orderDF = df
                .select(from_json(col("value").cast("string"), orderSchema).alias("parsed_value"))
                .select("parsed_value.*");

//        groupWithBankNo(orderDF);
        groupWithMerchantId(orderDF, spark);
//        groupWithAgentId(orderDF, spark);
//        groupWithGroupId(orderDF, spark);

        spark.streams().awaitAnyTermination();

    }

    private static void groupWithBankNo(Dataset<Row> orderDF) throws TimeoutException {
        Dataset<Row> countDf = orderDF.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("bank_no").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("bank_no"));
//        write2kafka(countDf);
        write2console(countDf);
    }


    private static void groupWithMerchantId(Dataset<Row> orderDF, SparkSession ss) throws TimeoutException {
        Dataset<Row> filterDf = ss
                .read()
                .format("jdbc")
                .option("driver", mysqlDriver)
                .option("url", mysqlUrl)
                .option("dbtable", "filter_merchant_ids")
                .option("user", userName)
                .option("password", password)
                .load();
        Dataset<Row> afterFilterDf = filterDf.where("org_type= '1'").join(orderDF, col("merchant_id"), "left");
        Dataset<Row> countDf = afterFilterDf.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("merchant_id").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("merchant_id"));
// Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode
// Default trigger (runs micro-batch as soon as it can)
//                .trigger(Trigger.ProcessingTime("1 minutes"))

//        write2kafka(countDf);
        write2console(countDf);
    }

    private static void groupWithAgentId(Dataset<Row> orderDF, SparkSession ss) throws TimeoutException {
        Dataset<Row> filterDf = ss
                .read()
                .format("jdbc")
                .option("driver", oracleDriver)
                .option("url", oracleUrl)
                .option("dbtable", "filter_merchant_ids")
                .option("user", userName)
                .option("password", password)
                .load();

        Dataset<Row> afterFilterDf = orderDF.join(filterDf, col("agent_id"), "left");
        Dataset<Row> countDf = afterFilterDf.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("agent_id").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("agent_id"));
// Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode
// Default trigger (runs micro-batch as soon as it can)
//                .trigger(Trigger.ProcessingTime("1 minutes"))
        write2kafka(countDf);

    }

    private static void groupWithGroupId(Dataset<Row> orderDF, SparkSession ss) throws TimeoutException {
        Dataset<Row> filterDf = ss
                .read()
                .format("jdbc")
                .option("driver", oracleDriver)
                .option("url", oracleUrl)
                .option("dbtable", "filter_group_ids")
                .option("user", userName)
                .option("password", password)
                .load();

        Dataset<Row> afterFilterDf = orderDF.join(filterDf, "group_id");
        Dataset<Row> countDf = afterFilterDf.
                withWatermark("trade_time", "30 seconds").
                groupBy(window(col("trade_time"), "1 minutes"), col("group_id").alias("groupBy"), col("term_type"), col("state")
                ).count().withColumn("groupByType", lit("group_id"));
// Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode
// Default trigger (runs micro-batch as soon as it can)
//                .trigger(Trigger.ProcessingTime("1 minutes"))
        write2kafka(countDf);
    }

    private static void write2kafka(Dataset<Row> countDf) throws TimeoutException {
        countDf
                .toJSON().as("value")
                .select(col("value").cast("string"))
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaIp)
                .option("topic", kafkaWriteTopic)
//                .trigger(Trigger.ProcessingTime("1 minutes"))
                .outputMode("append")
                .option("checkpointLocation", "C://tmp//dir//")
                .start();
    }

    private static void write2console(Dataset<Row> countDf) throws TimeoutException {
        countDf.writeStream().outputMode("append").format("console").start();
    }
}

