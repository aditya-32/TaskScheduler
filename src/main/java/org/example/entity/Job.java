package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.example.SchedulerService;
import org.example.enums.JobType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public class Job implements Runnable, Comparable<Job> {
    List<Task> tasks;
    Date startTime;
    TimeUnit timeUnit;
    Integer interval;
    JobType type;
    List<List<Task>> taskSequence;
    private List<List<Integer>> dependencyGraph;

    public static class JobBuilder {
        private List<Task> tasks;
        private Date startTime;
        private TimeUnit timeUnit;
        private Integer interval;
        private JobType type = JobType.ONE_TIME;
        private List<List<Integer>> dependencyGraph = new ArrayList<>();
        public JobBuilder tasks(List<Task> tasks) {
            this.tasks = tasks;
            return this;
        }
        public JobBuilder startTime(Date startTime) {
            this.startTime = startTime;
            return this;
        }
        public JobBuilder interval(Integer interval) {
            this.interval = interval;
            this.type = JobType.RECURRING;
            return this;
        }
        public JobBuilder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public JobBuilder dependencyGraph (List<List<Integer>> dependencyGraph) {
            this.dependencyGraph = dependencyGraph;
            return this;
        }

        public Job build() {
            Map<Integer, Integer> inDegree = new HashMap<>();
            Map<Integer, Set<Integer>> adj = new HashMap<>();
            Map<Integer, Task> taskMap = new HashMap<>();
            for (var task : tasks) {
                taskMap.put(task.getId(), task);
            }
            for (var list : dependencyGraph) {
                int u = list.get(0);
                int v = list.get(1);
                inDegree.put(v, inDegree.getOrDefault(v, 0) + 1);
                var set = adj.getOrDefault(u, new HashSet<>());
                set.add(v);
                adj.put(u, set);
            }
            Queue<Task> q = new ArrayDeque<>();
            Set<Integer> visited = new HashSet<>();
            for (var task : tasks) {
                if (inDegree.getOrDefault(task.getId(), 0) == 0) {
                    q.add(task);
                    visited.add(task.getId());
                }
            }
            List<List<Task>> taskPriority = new ArrayList<>();
            while(!q.isEmpty()) {
                int sz = q.size();
                List<Task> taskList = new ArrayList<>();
                for (int k=0;k<sz;k++) {
                    Task currTask = q.poll();
                    taskList.add(currTask);
                    for (var child : adj.getOrDefault(currTask.getId(), new HashSet<>())) {
                        if (!visited.contains(child)) {
                            visited.add(child);
                            q.add(taskMap.get(child));
                        }
                    }
                }
                taskPriority.add(taskList);
            }
            return new Job(tasks, startTime, timeUnit, interval, type, taskPriority, dependencyGraph);
        }
    }

    @Override
    public void run() {
        ExecutorService executor = Executors.newCachedThreadPool();
        for (Collection<Task> taskList : taskSequence) {
            var taskExecutables = taskList.stream().map(Task::getExecutable).toList();
            System.out.println("Parallel execution "+ taskList.stream().map(Task::getId).collect(Collectors.toSet()));
            try {
                List<Future<Void>> futures = executor.invokeAll(taskExecutables);
                for (var future : futures) {
                    future.get();
                }
            } catch (InterruptedException e) {

            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        if (type.equals(JobType.RECURRING)) {
            var currentTimer = Calendar.getInstance().getTime();
            Date newStartTimer = Date.from(currentTimer.toInstant().plus(getInterval(), timeUnit.toChronoUnit()));
            SchedulerService.getINSTANCE().scheduleTaskWithInterval(tasks, newStartTimer, interval, timeUnit, dependencyGraph);
        }
    }

    @Override
    public int compareTo(Job o) {
        return (int) (this.getStartTime().toInstant().toEpochMilli()
                - o.getStartTime().toInstant().toEpochMilli());
    }
}
