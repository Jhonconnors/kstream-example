package com.example.kstream.demo.context;

import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import com.example.kstream.demo.service.GeneratorService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Retryable
@Data
@Component
public class GeneratorContex {

    protected ProcessContext<Insights, Generator> processContext = new ProcessContext<>();
    @Autowired
    private GeneratorService generatorService;

    public void execute(ProcessContext<Insights, Generator> processContext){
        Generator generator = generatorService.generateGenerator(processContext.getInput());
        processContext.setOutput(generator);

        if (processContext.getInput().getId() ==12){
            Task task = new Task();
            task.setState(1);
            processContext.setTask(task);
        }
        System.out.println("Context : "+processContext);

    }

}
