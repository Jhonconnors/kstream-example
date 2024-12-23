package com.example.kstream.demo.service;

import com.example.kstream.demo.context.ProcessContext;
import com.example.kstream.demo.context.TaskElement;
import com.example.kstream.demo.model.ClientInsight;
import com.example.kstream.demo.model.SmsNotification;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class StreamProcessingService {

    public void process(ProcessContext<ClientInsight, SmsNotification> processContext) {
        List<TaskElement<ProcessContext<ClientInsight, SmsNotification>>> taskElements =
        processContext.getTask().getTaskExecutes();
        taskElements.forEach(t -> t.execute(processContext));
    }
}
