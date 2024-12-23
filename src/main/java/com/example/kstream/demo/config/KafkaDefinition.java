package com.example.kstream.demo.config;


import com.example.kstream.demo.model.TypeOperation;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
@Data
public class KafkaDefinition {
    @Value("${app.type-operation:TOPIC}")
    private TypeOperation typeOperation;

    @Value("${custom.topics.input}")
    private String inputTopic;

    @Value("${custom.topics.output}")
    private String outputTopicAdvice;

    @Value("${custom.topics.retry}")
    private String retryTopic;

    @Value("${custom.topics.error}")
    private String errorTopic;
}
