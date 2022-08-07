package com.zhoujing.flink.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./prod?id=6", 2000L),
                new Event("Mary", "./prod?id=7", 1000L),
                new Event("Bob", "./cat", 3000L),
                new Event("Bob", "./prod?id=5", 4000L),
                new Event("Bob", "./home", 2000L),
                new Event("Mary", "./prod?id=2", 4000L),
                new Event("Mary", "./prod?id=1", 3000L),
                new Event("Bob", "./prod?id=1", 2000L)
        );

        // 按键分组之后进行聚合，提取当前用户最近访问数据
        // max 和 maxby的区别： maxby 返回的是最新的完整数据，max 仅保证max的那个字段的数据是最新的
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max");


        stream.keyBy(e -> e.user).maxBy("timestamp").print("maxby");

        env.execute();


    }


}
