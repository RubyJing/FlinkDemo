package com.zhoujing.flink.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> fromElementsStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cat", 2000L)
        );

        // 1. 实现自定义的FlatMapFunction的接口
        fromElementsStream.flatMap(new MyFlatMap()).print();

        //2. 传入一个Lambda表达式
        fromElementsStream.flatMap((Event event, Collector<String> out) -> {
            if ("Mary".equals(event.user)) {
                out.collect(event.user);
            } else if ("Bob".equals(event.user)) {
                out.collect(event.user);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {})
                .print("lambda:");

        env.execute();
    }

    /**
     * 实现一个自定义的 FlatMapFunction
     */
    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());
        }
    }

}
