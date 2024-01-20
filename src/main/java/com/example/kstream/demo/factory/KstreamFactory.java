package com.example.kstream.demo.factory;

import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.Task;
import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import com.example.kstream.demo.service.StreamProcessingService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.kstream.Produced;


@Component
public class KstreamFactory {

    @Value("${custom.topics.input}")
    private String inputTopic;

    @Value("${custom.topics.output}")
    private String oututTopicAdvice;

    @Value("${custom.topics.retry}")
    private String retryTopic;

    @Value("${custom.topics.error}")
    private String errorTopic;


    @Autowired
    private StreamProcessingService streamProcessingService;

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        KStream<String, ProcessContext<Insights, Generator>> stream = builder.stream(inputTopic,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Insights.class)))
                .filter((key, value) -> value != null)
                .mapValues(streamProcessingService::process);
        sendToTopicBasedOnCondition(stream, ProcessContext.class);
    }

    public <T extends ProcessContext<?, ?>, O extends ProcessContext<?, ?>> void sendToTopicBasedOnCondition(KStream<String, T> stream, Class<O> contextClass) {
        stream.split()
                .branch((key, value) -> value.getTask().getState() == Task.State.Success.getValue(),
                        Branched.withConsumer(ks -> ks
                                .mapValues((context) -> {
                                    System.out.println("En el primer branch ...");
                                    return context;
                                })
                                .to(oututTopicAdvice, (Produced<String, T>) Produced.with(Serdes.String(), new JsonSerde<>(contextClass)))))
                .branch((key, value) -> value.getTask().getState() == Task.State.Error.getValue(),
                        Branched.withConsumer(ks -> ks
                                .to(retryTopic, Produced.with(Serdes.String(), new JsonSerde<>(ProcessContext.class)))))
                .branch((key, value) -> value.getTask().getState() == Task.State.Error.getValue(),
                        Branched.withConsumer(ks -> ks
                                .to(errorTopic, Produced.with(Serdes.String(), new JsonSerde<>(ProcessContext.class)))));
    }

}