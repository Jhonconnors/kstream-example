package com.example.kstream.demo.model;

public enum TaskState {

    Retry(3),
    Error(2),
    Success(1),
    Running(4),
    Stopping(5);

    // Atributo que almacena el valor numérico de cada estado
    private final int value;

    // Constructor del enum para asignar el valor
    TaskState(int value) {
        this.value = value;
    }

    // Método para obtener el valor asociado al estado
    public int getValue() {
        return value;
    }

    // Método estático opcional para obtener el estado por su valor
    public static TaskState fromValue(int value) {
        for (TaskState state : TaskState.values()) {
            if (state.getValue() == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unexpected value: " + value);
    }
}
