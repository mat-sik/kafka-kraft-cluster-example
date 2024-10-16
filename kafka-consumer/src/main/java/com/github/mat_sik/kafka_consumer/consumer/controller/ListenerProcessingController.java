package com.github.mat_sik.kafka_consumer.consumer.controller;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public class ListenerProcessingController {

    private final Lock writeLock;
    private final AtomicBoolean shouldProcess;

    public ListenerProcessingController(Lock writeLock, AtomicBoolean shouldProcess) {
        this.writeLock = writeLock;
        this.shouldProcess = shouldProcess;
    }

    public void lock() throws InterruptedException {
        writeLock.lockInterruptibly();
    }

    public void unlock() {
        writeLock.unlock();
    }

    public void shouldNotProcess() {
        shouldProcess.set(false);
    }

    public void shouldProcess() {
        shouldProcess.set(true);
    }

}
