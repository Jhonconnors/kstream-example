package com.example.kstream.demo.context;

import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import lombok.Data;

@Data
public class GeneratorProcessContext extends ProcessContext<Insights, Generator>{

    private Long id;
}
