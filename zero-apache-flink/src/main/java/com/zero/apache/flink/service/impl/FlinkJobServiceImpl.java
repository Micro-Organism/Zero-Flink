package com.zero.apache.flink.service.impl;

import com.zero.apache.flink.service.FlinkJobService;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;

@Service
public class FlinkJobServiceImpl implements FlinkJobService {

    @Override
    public void runFlinkJob() throws Exception {
        // 初始化执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源
        DataStream<String> text = env.fromElements("Hello", "Flink", "Spring Boot");

        // 数据处理
        DataStream<String> processedText = text.map(new MapFunction<String, String>() {
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
