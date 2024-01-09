package com.example.kstream.demo.factory;

import com.example.kstream.demo.config.serde.AdviceDeserializer;
import com.example.kstream.demo.config.serde.AdviceSerializer;
import com.example.kstream.demo.config.serde.InsightsDeserializer;
import com.example.kstream.demo.config.serde.InsightsSerializer;
import com.example.kstream.demo.model.Advice;
import com.example.kstream.demo.model.Insights;
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

    private static final Serde<String> STRING_SERDE = Serdes.String();


    @Value("${custom.topics.input}")
    private String inputTopic;

    @Value("${custom.topics.output}")
    private String oututTopicAdvice;

    private static final Serde<Insights> INSIGHTS_SERDE = Serdes.serdeFrom(new InsightsSerializer(), new InsightsDeserializer());
    private static final Serde<Advice> ADVICE_SERDE = Serdes.serdeFrom(new AdviceSerializer(), new AdviceDeserializer());

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Insights> inputStream = streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), INSIGHTS_SERDE));

        inputStream.filter((key, value) -> value != null)
        .mapValues(value -> {
            System.out.println("Received Insights: " + value);
            // Realizar cualquier lógica de filtrado o transformación para generar Advice
            Advice advice = generateAdvice(value); // Función que genera Advice a partir de Insights
            return advice;
        })
                .filter((key, value) -> value != null) // Filtrar si el Advice es nulo
                .to(oututTopicAdvice, Produced.with(Serdes.String(), ADVICE_SERDE));
    }

    private Advice generateAdvice(Insights insights) {
        Advice advice = new Advice();
        advice.setIdBala(insights.getId());
        advice.setZona(insights.getVariante());
        return advice;
    }

}