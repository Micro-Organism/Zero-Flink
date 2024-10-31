package com.zero.apache.flink;

import com.zero.apache.flink.common.job.DataSetBatch;
import com.zero.apache.flink.common.util.MyFlatMapper;
import com.zero.apache.flink.service.FlinkJobService;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ZeroApacheFlinkApplicationTests {

    @Autowired
    private FlinkJobService flinkJobService;

    @Test
    void contextLoads() {
//        // 获取 FlinkJobService Bean，并运行 Flink 作业
//        FlinkJobService flinkJobService = context.getBean(FlinkJobService.class);
//        flinkJobService.runFlinkJob();

        String path1 = System.getProperty("user.dir");
        System.out.println(path1);
        String path2 = System.getProperties().getProperty("user.dir");
        System.out.println(path2);
    }

    @Test
    void testDataSetBatch() throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 获取文件路径
        String path = DataSetBatch.class.getClassLoader().getResource("word.txt").getPath();
        // 从文件中读取数据
        DataSource<String> lineDS = env.readTextFile(path);

        // 切分、转换，例如： （word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new MyFlatMapper());

        // 按word分组 按照第一个位置的word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);

        // 分组内聚合统计 将第二个位置上的数据求和
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);

        // 输出
        sum.print();
    }

    @Test
    void testDataStreamAPI() throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从项目根目录下的data目录下的word.txt文件中读取数据
        DataStreamSource<String> source = env.readTextFile(System.getProperty("user.dir") + "\\src\\main\\resources\\static\\word.txt");

        // 处理数据: 切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 按空格切分
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // 转换成二元组 （word，1）
                            Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                            // 通过采集器向下游发送数据
                            out.collect(wordsAndOne);
                        }
                    }
                });

        // 处理数据:分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        // 处理数据:聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        // 输出数据
        sumDS.print();

        // 执行
        env.execute();
    }

    /**
     * 并行度优先级：算子 > 全局env > 提交指定 > 配置文件
     */
    @Test
    void testFlinkWebUI() throws Exception {
        // 本地模式
        Configuration conf = new Configuration();
        // 指定端口
        conf.setString(RestOptions.BIND_PORT, "7777");
        //  创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 全局指定并行度，默认是电脑的线程数
        env.setParallelism(2);

        // 读取socket文本流
        DataStreamSource<String> socketDS = env.socketTextStream("172.24.4.193", 8888);

        //  处理数据: 切割、转换、分组、聚合 得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                // 局部设置算子并行度
                .setParallelism(3)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                // 局部设置算子并行度
                .setParallelism(4);

        //  输出
        sum.print();

        //  执行
        env.execute();
    }

    @Test
    void testSocketJob() throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定并行度,默认电脑线程数
        env.setParallelism(3);
        // 读取数据socket文本流 指定监听 IP 端口 只有在接收到数据才会执行任务
        DataStreamSource<String> socketDS = env.socketTextStream("172.24.4.193", 8888);

        // 处理数据: 切换、转换、分组、聚合 得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .setParallelism(2)
                // // 显式地提供类型信息:对于flatMap传入Lambda表达式，系统只能推断出返回的是Tuple2类型，而无法得到Tuple2<String, Long>。只有显式设置系统当前返回类型，才能正确解析出完整数据
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
//                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);


        // 输出
        sum.print();

        // 执行
        env.execute();
    }

    @Test
    void testStringProcessingJob() throws Exception {

        // 初始化执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStream<String> text = env.fromElements("Hello", "Flink", "Spring Boot");

        // 数据处理
        DataStream<String> processedText = text
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "Processed: " + value;
                    }
                });

        // 输出结果
        processedText.print();

        // 执行作业
        env.execute("String Processing Job");

    }

}
