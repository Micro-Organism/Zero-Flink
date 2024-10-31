package com.zero.apache.flink.service;

public interface FlinkJobService {
    /**
     * Run flink job
     * @throws Exception   异常
     */
    void runFlinkJob() throws Exception;
}
