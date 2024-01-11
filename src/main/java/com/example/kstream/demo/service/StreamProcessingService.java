package com.example.kstream.demo.service;

import com.example.kstream.demo.config.serde.AdviceDeserializer;
import com.example.kstream.demo.config.serde.AdviceSerializer;
import com.example.kstream.demo.config.serde.ProcessContextSerializer;
import com.example.kstream.demo.context.GeneratorContex;
import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamProcessingService {

    @Autowired
    private GeneratorContex generatorContex;

    private static final Serde<ProcessContext<Insights, Generator>> PROCESS_CONTEXT_SERDE =
            Serdes.serdeFrom(new ProcessContextSerializer<>(), new ProcessContextSerializer<>());
    private static final Serde<Generator> ADVICE_SERDE = Serdes.serdeFrom(new AdviceSerializer(), new AdviceDeserializer());

    public ProcessContext<Insights, Generator> process(Insights value) {
        System.out.println("Received Insights: " + value);

        ProcessContext<Insights, Generator> processContext = new ProcessContext<>();
        processContext.setInput(value);

        generatorContex.setProcessContext(processContext);
        generatorContex.execute(processContext);

        return processContext;
    }

    public void routeToBranches(KStream<String, ProcessContext<Insights, Generator>> adviceStream) {
        KStream<String, ProcessContext<Insights, Generator>>[] branches = adviceStream.branch(
                (key, value) -> value.getOutput() == null,
                (key, value) -> value.getErrorMessage() != null,
                (key, value) -> value.getOutput() != null
        );

        branches[0].to("error-insight", Produced.with(Serdes.String(), PROCESS_CONTEXT_SERDE));
        branches[1].to("retry-insight", Produced.with(Serdes.String(), PROCESS_CONTEXT_SERDE));
        branches[2].mapValues(value -> value.getOutput())
                .to("advice", Produced.with(Serdes.String(), ADVICE_SERDE));
    }
}
