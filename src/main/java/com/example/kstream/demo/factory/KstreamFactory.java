package com.example.kstream.demo.factory;

import com.example.kstream.demo.config.serde.*;
import com.example.kstream.demo.context.GeneratorContex;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import com.example.kstream.demo.service.GeneratorService;
import com.example.kstream.demo.service.StreamProcessingService;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.kstream.Produced;


@Component
public class KstreamFactory {

    @Value("${custom.topics.input}")
    private String inputTopic;

    @Value("${custom.topics.output}")
    private String oututTopicAdvice;

    private static final Serde<Insights> INSIGHTS_SERDE = Serdes.serdeFrom(new InsightsSerializer(), new InsightsDeserializer());

    @Autowired
    private StreamProcessingService streamProcessingService;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Insights> inputStream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), INSIGHTS_SERDE));

        KStream<String, ProcessContext<Insights, Generator>> adviceStream = inputStream
                .filter((key, value) -> value != null)
                .mapValues(streamProcessingService::process)
                .filter((key, value) -> value != null);

        streamProcessingService.routeToBranches(adviceStream);
    }


}