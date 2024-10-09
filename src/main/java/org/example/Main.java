package org.example;

import org.example.entity.Task;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        var instance = SchedulerService.getINSTANCE();
        Callable<Void> runnable1 = () -> {
            System.out.println("Task1");
            return null;
        };
        Callable<Void> runnable2 = () -> {
            System.out.println("Task2");
            return null;
        };

        Callable<Void> runnable3 = () -> {
            System.out.println("Task3");
            return null;
        };

        Task task1 = Task.builder()
                .id(1)
                .executable(runnable1)
                .build();
        Task task2 = Task.builder()
                .id(2)
                .executable(runnable2)
                .build();

        Task task3 = Task.builder()
                .id(3)
                .executable(runnable3)
                .build();
        var startTime = new Date().toInstant().plus(5, ChronoUnit.SECONDS);
        List<List<Integer>> dependecyList = new ArrayList<>();
        dependecyList.add(List.of(1,2));
        dependecyList.add(List.of(1,3));
//        instance.scheduleTask(Collections.singletonList(task1), Date.from(startTime));
        instance.scheduleTaskWithInterval(List.of(task1, task2, task3), Date.from(startTime), 3, TimeUnit.SECONDS, dependecyList);
    }
}