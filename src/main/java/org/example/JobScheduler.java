package org.example;

import org.example.entity.Job;

import java.util.Calendar;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class JobScheduler implements Runnable{
    private final ExecutorService excutors;
    private final PriorityQueue<Job> jobStore;
    private final Lock lock;
    private final Condition entryAdded;

    public JobScheduler(PriorityQueue<Job> jobStore,Condition entryAdded, Lock lock, Integer threadCount) {
        this.jobStore = jobStore;
        this.lock = lock;
        this.entryAdded = entryAdded;
        this.excutors = Executors.newFixedThreadPool(threadCount);
    }

    @Override
    public void run() {
        while(true) {
            try {
                lock.lock();
                while (jobStore.isEmpty()) {
                    entryAdded.await();
                }
                var job = jobStore.peek();
                var currentTimer = Calendar.getInstance().getTime();
                if (currentTimer.compareTo(job.getStartTime()) >=0 ) {
                    jobStore.remove();
                    excutors.execute(job);
                }else {
                    var delay = job.getStartTime().getTime() - currentTimer.getTime();
                    entryAdded.await(delay, TimeUnit.MILLISECONDS);
                }
            }catch (Exception e){

            }finally {
                lock.unlock();
            }
        }
    }
}
