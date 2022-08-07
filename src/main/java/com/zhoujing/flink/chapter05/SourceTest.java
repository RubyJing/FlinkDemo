package com.zhoujing.flink.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        //2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cat", 2000L));
        DataStreamSource<Event> eventStream = env.fromCollection(events);

        //3. 从元素读取数据
        DataStreamSource<Event> fromElementsStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cat", 2000L)
        );

        //4. 从Socket文本流处理
        DataStreamSource<String> hadoop102Stream = env.socketTextStream("hadoop102", 7777);

        //5. 从Kafka中读取数据
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092");
        props.setProperty("group.id", "consumer-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), props));
        kafkaStream.print("kafkaStream");

        numStream.print("nums");
        eventStream.print("eventStream");
        stream1.print("stream1");
        fromElementsStream.print("fromElementsStream");
        hadoop102Stream.print("hadoop102Stream");


        env.execute();

    }

}
