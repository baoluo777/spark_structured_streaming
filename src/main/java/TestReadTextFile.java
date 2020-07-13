import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;


import java.util.concurrent.TimeoutException;

public class TestReadTextFile {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        StructType userSchema = new StructType().add("name", "string").add("age", "int").add("timestamp","timestamp");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema)      // Specify schema of the csv files
                .csv("C://tmp//");

        Dataset<Row> windowedCounts = csvDF.groupBy(
                window(col("timestamp"), "10 minutes"),
                col("name")
        ).count();
        Dataset<Row> windowedCounts1 = csvDF.filter("age > 20").groupBy(
                window(col("timestamp"), "10 minutes"),
                col("name")
        ).count();
         windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
         windowedCounts1.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
//        query.awaitTermination();

        spark.streams().awaitAnyTermination();
    }
}
