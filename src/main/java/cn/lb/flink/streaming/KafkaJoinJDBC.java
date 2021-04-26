package cn.lb.flink.streaming;

import cn.lb.flink.streaming.streaming.pojo.MysqlTestTable;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * 更详细的外部表Join可参考：https://www.cnblogs.com/qiu-hua/p/13870992.html
 * https://blog.csdn.net/weixin_43315211/article/details/90786628
 * https://github.com/dafei1288/flink_casestudy/blob/master/topn_demo/src/main/java/cn/flinkhub/topn/JoinApp.java
 * 包括Hbase redis，缓存表等
 */

public class KafkaJoinJDBC {
    private static final String kafkaIp = "127.0.0.1:9092";
    private static final String kafkaWriteTopic = "monitorOrder";
    private static final String mysqlDriver = "com.mysql.jdbc.Driver";
    private static final String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/monitor?useUnicode=true&characterEncoding=utf8";
    private static final String userName = "root";
    private static final String password = "12345678";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Row> jss = getJDBCDataStreamSource(env);
        DataStream<String> kds = getKafkaDataStreamSource(env);
//        JoinedStreams<Row, ObjectNode> ds=jss.join(kds);
        env.execute();
    }
    private static DataStream<String>  getKafkaDataStreamSource(StreamExecutionEnvironment env)   {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStreamSource<String> kafkaStream = env
                .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
        return kafkaStream;
    }
    private static DataStreamSource<Row> getJDBCDataStreamSource(StreamExecutionEnvironment env) {
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(mysqlDriver)
                .setDBUrl(mysqlUrl)
                .setUsername(userName)
                .setPassword(password)
                .setQuery("select org_type,merchant_id,id from filter_merchant_ids")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStreamSource<Row> ds = env.createInput(jdbcInputFormat);
        return ds;
    }
}
