package com.zhoujing.flink.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> fromElementsStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cat", 2000L)
        );

        // 进行转换计算，提取user字段

        // 1. 自定义FilterFunction类
        SingleOutputStreamOperator<Event> filter = fromElementsStream.filter(new MyFilter());
        filter.print("filter:");

        //2. 匿名类实现
        SingleOutputStreamOperator<Event> filter2 = fromElementsStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "Mary".equals(event.user);
            }
        });
        filter2.print("filter2:");

        //3. lambda表达式实现
        fromElementsStream.filter(e -> "Mary".equals(e.user)).print("lambda:");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return "Mary".equals(event.user);
        }
    }
}
