package com.example.kstream.demo.context;

import com.example.kstream.demo.model.TaskState;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.List;

@Data
@Component
public class TaskChain<E extends TaskElement> {

    @Autowired
    private List<E> taskExecutes;
    private TaskState state = TaskState.Running;

}
