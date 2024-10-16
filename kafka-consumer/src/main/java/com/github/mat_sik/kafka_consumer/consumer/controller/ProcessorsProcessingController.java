package com.github.mat_sik.kafka_consumer.consumer.controller;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public class ProcessorsProcessingController {

    private final Lock readLock;
    private final AtomicBoolean shouldProcess;

    public ProcessorsProcessingController(Lock readLock, AtomicBoolean shouldProcess) {
        this.readLock = readLock;
        this.shouldProcess = shouldProcess;
    }

    public void lock() throws InterruptedException {
        readLock.lockInterruptibly();
    }

    public void unlock() {
        readLock.unlock();
    }

    public boolean shouldProcess() {
        return shouldProcess.get();
    }

}
