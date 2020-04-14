package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;

public class domainFrequency {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fsEnv.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        String filename = "/Users/whisp/Mycode/dns_anomaly_detect/logprocess/hitwh_process/nohup.out";
        //源数据流中内容为 epoch_time, full_domain, domain_fld, query_client_ip
        SingleOutputStreamOperator<Tuple4<Long, String, String, String>> source = fsEnv
                .readTextFile(filename)
                /*
                .addSource(new FlinkKafkaConsumer010<String>(
                            "queries",
                            new SimpleStringSchema(),
                            properties
                        )
                )

                 */
                .map(new MapFunction<String, Tuple4<Long, String, String, String>>() {
                    @Override
                    public Tuple4<Long, String, String, String> map(String s) throws Exception {
                        String[] singleQuery = s.split(",");
                        return new Tuple4<Long, String, String, String>(
                                Long.parseLong(singleQuery[0]),
                                singleQuery[1],
                                singleQuery[2],
                                singleQuery[3]
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Long, String, String, String>>(Time.seconds(1)){
                            @Override
                            public long extractTimestamp(Tuple4<Long, String, String, String> longStringIntegerTuple4) {
                                return longStringIntegerTuple4.f0 * 1000;
                            }
                        }
                );

        tableEnv.registerDataStream("tdns", source, "rowtime.rowtime, full_domain, domain_fld, query_client_ip");

        //result1 监控 full domain 的出现频率
        Table result1 = tableEnv.sqlQuery(
                "SELECT HOP_START(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE) as rstart, " +
                        "full_domain, COUNT(full_domain) FROM tdns " +
                        "GROUP BY HOP(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE), full_domain " +
                        "HAVING COUNT(full_domain)>5"
        );

        //result2 监控每一个domain_fld有多少活跃子域名
        Table result2 = tableEnv.sqlQuery(
                "SELECT HOP_START(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE) as rstart, " +
                        "domain_fld, COUNT(DISTINCT(full_domain)) FROM tdns " +
                        "GROUP BY HOP(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE), domain_fld"
        );

        //result3 监控每一个client_ip的查询情况
        Table result3 = tableEnv.sqlQuery(
                "SELECT HOP_START(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE) as rstart, " +
                        "query_client_ip, COUNT(DISTINCT(full_domain)) FROM tdns " +
                        "GROUP BY HOP(rowtime, INTERVAL '20' SECOND, INTERVAL '1' MINUTE), query_client_ip"
        );



        DataStream ds1 = tableEnv.toRetractStream(result1, Row.class);
        DataStream ds2 = tableEnv.toRetractStream(result2, Row.class);
        DataStream ds3 = tableEnv.toRetractStream(result3, Row.class);

        ds1.writeAsCsv("fullDomainFrequencyResult");
        ds2.writeAsCsv("activeSubdomainsResult");
        ds3.writeAsCsv("clientIPBehaviorResult");

        fsEnv.execute("FullDomain WordCount");
    }
}