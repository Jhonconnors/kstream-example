package com.example.kstream.demo.service;

import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.Task;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

public class BuilderService {

    public <O, T extends ProcessContext<?, O>> void splitStream
            (KStream<String, T> stream, String oututTopicAdvice, String retryTopic, String errorTopic, Class<T> processContext,  Class<O> output) {
        stream.split()
                .branch((key, value) -> value.getTask().getState() == Task.State.Success.getValue(),
                        Branched.withConsumer(ks -> ks
                                .mapValues((context) -> {
                                    System.out.println("En el primer branch ...");
                                    return context.getOutput();
                                })
                                .to(oututTopicAdvice, Produced.with(Serdes.String(), new JsonSerde<>(output)))))
                .branch((key, value) -> value.getTask().getState() == Task.State.Retry.getValue(),
                        Branched.withConsumer(ks -> ks
                                .to(retryTopic, Produced.with(Serdes.String(), new JsonSerde<>(processContext)))))
                .branch((key, value) -> value.getTask().getState() == Task.State.Error.getValue(),
                        Branched.withConsumer(ks -> ks
                                .map((k, v) -> KeyValue.pair(v.getKey(), v))
                                .to(errorTopic, Produced.with(Serdes.String(), new JsonSerde<>(processContext)))));
    }

    public  <Q extends ProcessContext<?, ?>> Long getId(Q process){
        System.out.println("This is the Process : "+process);
        return 5L;
    }
}
