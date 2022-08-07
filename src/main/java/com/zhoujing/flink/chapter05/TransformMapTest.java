package com.zhoujing.flink.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> fromElementsStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cat", 2000L)
        );

        // 进行转换计算，提取user字段

        //1. 自定义类
        SingleOutputStreamOperator<String> result = fromElementsStream.map(new MyMapper());
        //2. 匿名函数类实现
        SingleOutputStreamOperator<String> result2 = fromElementsStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });
        //3. lambda表达式
        SingleOutputStreamOperator<String> result3 = fromElementsStream.map(e -> e.user);

        result.print("result");
        result2.print("result2");
        result3.print("result3");

        env.execute();

    }

    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }

}
