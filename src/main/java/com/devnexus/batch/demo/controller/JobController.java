package com.devnexus.batch.demo.controller;

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
  public void triggerJob(@PathVariable String jobName) {
    Job job = (Job) context.getBean(jobName);
    try {
      jobLauncher.run(job, new JobParametersBuilder().addString("now", LocalDateTime.now().toString()).toJobParameters());
    } catch (JobExecutionAlreadyRunningException e) {
      log.warn("JobExecutionAlreadyRunningException " + e);
    } catch (JobRestartException e) {
      log.warn("JobRestartException " + e);
    } catch (JobInstanceAlreadyCompleteException e) {
      log.warn("JobInstanceAlreadyCompleteException " + e);
    } catch (JobParametersInvalidException e) {
      log.warn("JobParametersInvalidException " + e);
    }
  }
}
