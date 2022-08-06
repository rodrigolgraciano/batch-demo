package com.rh.batch.demo.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

/**
 * Endpoint to listen for job requests.
 * Open to discuss
 * <li>JobParameters</li>
 * <li>Sync x Async Job launchers</li>
 * <li>Job exceptions</li>
 */
@RestController("/job")
public class JobController {

  private final JobLauncher asyncJobLauncher;
  private final JobLauncher jobLauncher;
  private final ApplicationContext context;
  private static final Logger log = LoggerFactory.getLogger(JobController.class);

  public JobController(@Qualifier("asyncJobLauncher") JobLauncher asyncJobLauncher, @Qualifier("jobLauncher") JobLauncher jobLauncher, ApplicationContext context) {
    this.asyncJobLauncher = asyncJobLauncher;
    this.jobLauncher = jobLauncher;
    this.context = context;
  }

  @GetMapping("/trigger/{jobName}")
  public String triggerJob(@PathVariable String jobName) {
    return executeJob( jobName, jobLauncher );
  }

  @GetMapping("/trigger-async/{jobName}")
  public String triggerAsyncJob(@PathVariable String jobName) {
    return executeJob( jobName, asyncJobLauncher );
  }

  private String executeJob(@PathVariable String jobName, JobLauncher jobLauncher) {
    Job job = (Job) context.getBean( jobName);
    try {
      jobLauncher.run( job, new JobParametersBuilder().addString( "now", LocalDateTime.now().toString()).toJobParameters());
    } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
      log.warn("Exception {} while running the job {}", e.getMessage(), jobName);
      return "500-Error";
    }
    return "200-OK";
  }
}
