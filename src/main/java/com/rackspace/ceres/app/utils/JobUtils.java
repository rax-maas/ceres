package com.rackspace.ceres.app.utils;

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.JobConfig;
import com.rackspace.ceres.app.config.JobTimerConfig;
import com.rackspace.ceres.app.model.Job;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
public class JobUtils {
    private final JobConfig jobConfig;
    private final JobTimerConfig jobTimers;
    private final DownsampleProperties properties;
    public String JOB_IS_ASSIGNED = "Job is assigned";
    public String JOB_IS_UNAVAILABLE = "Job is unavailable";
    public String JOB_IS_FREE = "Job is free";

    JobUtils(JobConfig jobConfig, JobTimerConfig jobTimers, DownsampleProperties properties) {
        this.jobConfig = jobConfig;
        this.jobTimers = jobTimers;
        this.properties = properties;
    }

    public String getJobInternal(Job job) {
        Job searchJob = new Job(job.getPartition(), job.getGroup(), "free");
        int index = this.jobConfig.jobList().indexOf(searchJob);
        if (index > -1) {
            log.info("claimJob: {}", job);
            claimJob(job, index);
            return JOB_IS_ASSIGNED;
        } else {
            Job searchJob2 = new Job(job.getPartition(), job.getGroup(), job.getStatus());
            index = this.jobConfig.jobList().indexOf(searchJob2);
            if (index > -1) {
                Instant then = this.jobTimers.jobTimers().get(index);
                if (Duration.between(then, Instant.now()).getSeconds() >
                        properties.getMaxDownsampleJobDuration().getSeconds()) {
                    log.info("jobTimed out: {}", searchJob2);
                    claimJob(job, index);
                    return JOB_IS_ASSIGNED;
                }
            }
            return JOB_IS_UNAVAILABLE;
        }
    }

    public String freeJobInternal(Job job) {
        int index = this.jobConfig.jobList().indexOf(job);
        if (index > -1) {
            this.jobConfig.jobList().set(index, new Job(job.getPartition(), job.getGroup(), "free"));
            log.info("Freeing job: {}", job);
            return JOB_IS_FREE;
        } else {
            return "Job is not found, but ok, I'll let it slide...";
        }
    }

    private void claimJob(@RequestBody Job job, int index) {
        this.jobConfig.jobList().set(index, new Job(job.getPartition(), job.getGroup(), job.getStatus()));
        this.jobTimers.jobTimers().set(index, Instant.now());
    }
}
