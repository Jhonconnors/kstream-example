package com.example.kstream.demo.factory;

import com.example.kstream.demo.context.GeneratorProcessContext;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import com.example.kstream.demo.service.BuilderService;
import com.example.kstream.demo.service.StreamProcessingService;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;


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

    private BuilderService builderService = new BuilderService();

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        ProcessContext<Insights, Generator> process = new ProcessContext<>();
        KStream<String, GeneratorProcessContext> stream = builder.stream(inputTopic,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Insights.class)))
                .filter((key, value) -> value != null)
                .mapValues((value1) -> {
                    process.setInput(value1);
                    process.setOutput(streamProcessingService.process(value1).getOutput());
                    streamProcessingService.process(value1);
                    return streamProcessingService.process(value1);
                        });

      builderService.splitStream(stream, oututTopicAdvice, retryTopic, errorTopic, GeneratorProcessContext.class, Generator.class);

      builderService.getId(process);

    }


}