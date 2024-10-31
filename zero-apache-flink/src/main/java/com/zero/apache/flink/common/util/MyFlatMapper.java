package com.zero.apache.flink.common.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 自定义MyFlatMapper类，实现FlatMapFunction接口
 * 输出: String 元组Tuple2<String, Integer>>  Tuple2是flink提供的元组类型
 */
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    //value是输入，out就是输出的数据
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // 按空格切分单词
        String[] words = value.split(" ");
        // 遍历所有word，包成二元组输出 将单词转换为 （word，1）
        for (String word : words) {
            Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
            //  使用Collector向下游发送数据
            out.collect(wordTuple2);
        }
    }
}
