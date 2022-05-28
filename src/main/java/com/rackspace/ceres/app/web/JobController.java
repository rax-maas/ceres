package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.model.Job;
import com.rackspace.ceres.app.utils.JobUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class JobController {
    private final JobUtils jobUtils;

    JobController(JobUtils jobUtils) {
        this.jobUtils = jobUtils;
    }

    @PostMapping("/api/job")
    public ResponseEntity<String> getJob(@RequestBody Job job) {
        String result = this.jobUtils.getJobInternal(job);
        if (result.equals(this.jobUtils.JOB_IS_ASSIGNED)) {
            return ResponseEntity.status(HttpStatus.OK).body(result);
        }
        return ResponseEntity.status(HttpStatus.IM_USED).body(result);
    }

    @PutMapping("/api/job")
    public ResponseEntity<String> freeJob(@RequestBody Job job) {
        return ResponseEntity.status(HttpStatus.OK).body(this.jobUtils.freeJobInternal(job));
    }
}
