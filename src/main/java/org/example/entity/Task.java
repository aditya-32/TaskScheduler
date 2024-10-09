package org.example.entity;

import lombok.Builder;
import lombok.Value;

import java.util.concurrent.Callable;

@Value
@Builder
public class Task implements Comparable<Task> {
    Integer id;
    Callable<Void> executable;

    public void execute() {
        try {
            executable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTo(Task o) {
        return this.id.compareTo(o.id);
    }
}
