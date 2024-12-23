package com.example.kstream.demo.context;


public abstract class TaskElement<C> {

    public abstract void execute(C context);
}
