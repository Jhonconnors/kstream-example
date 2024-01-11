package com.example.kstream.demo.service;

import com.example.kstream.demo.model.Generator;
import com.example.kstream.demo.model.Insights;
import org.springframework.stereotype.Service;

@Service
public class GeneratorService {

    public Generator generateGenerator(Insights insights) {
        Generator generator = new Generator();
        generator.setIdBala(insights.getId());
        generator.setZona(insights.getVariante());
        return generator;
    }
}
