package org.example;

import org.example.entity.Job;
import org.example.entity.Task;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SchedulerService {
    private static final SchedulerService INSTANCE = new SchedulerService(Runtime.getRuntime().availableProcessors()-1);

    private final PriorityQueue<Job> queue;
    private final Lock lock;
    private final Condition entryAdded;

    public static SchedulerService getINSTANCE() {
        return INSTANCE;
    }

    public SchedulerService(Integer threadCount) {
        this.queue = new PriorityQueue<>();
        this.lock = new ReentrantLock();
        this.entryAdded = lock.newCondition();
        Thread executor = new Thread(new JobScheduler(queue, entryAdded, lock, threadCount));
        executor.start();
    }

    public void scheduleTask(List<Task> tasks, Date startTime) {
        var job = new Job.JobBuilder()
                .tasks(tasks)
                .startTime(startTime)
                .timeUnit(TimeUnit.SECONDS)
                .build();
        scheduleJob(job);
    }

    private void scheduleJob(Job job) {
        lock.lock();
        try {
            queue.add(job);
            entryAdded.signal();
        }finally {
            lock.unlock();
        }
    }

    public void scheduleTaskWithInterval(List<Task> tasks, Date startTime, Integer delay, TimeUnit timeUnit, List<List<Integer>> dependencyGraph) {
        var job = new Job.JobBuilder()
                .tasks(tasks)
                .startTime(startTime)
                .timeUnit(timeUnit)
                .interval(delay)
                .dependencyGraph(dependencyGraph)
                .build();
        scheduleJob(job);
    }
}
