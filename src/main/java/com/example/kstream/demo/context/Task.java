package com.example.kstream.demo.context;

import lombok.Data;

@Data
public class Task {

    private String contextOld;
    private Integer State;

    public enum State {
        Retry(3),
        Error(2),
        Success(1),
        Running(4),
        Stopping(5);

        private final int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }


}
