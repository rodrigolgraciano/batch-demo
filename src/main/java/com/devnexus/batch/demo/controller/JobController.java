package com.devnexus.batch.demo.controller;

import com.devnexus.batch.demo.listener.JobCompletionNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController("/job")
public class JobController {

  private final JobLauncher launcher;
  private final ApplicationContext context;
  private static final Logger log = LoggerFactory.getLogger(JobController.class);



  public JobController(JobLauncher launcher, ApplicationContext context) {
    this.launcher = launcher;
    this.context = context;
  }

  @GetMapping("/trigger/{jobName}")
  public void triggerJob(@PathVariable String jobName) {
    Job job = (Job) context.getBean(jobName);
    try {
//      launcher.run(job, new JobParametersBuilder().addString("now", LocalDateTime.now().toString()).toJobParameters());
      launcher.run(job, new JobParametersBuilder().addString("jobName", jobName).toJobParameters());

    } catch (JobExecutionAlreadyRunningException e) {
      e.printStackTrace();
    } catch (JobRestartException e) {
      log.warn("JobRestartException " + e);
    } catch (JobInstanceAlreadyCompleteException e) {
      log.warn("JobInstanceAlreadyCompleteException " + e);
      e.printStackTrace();
    } catch (JobParametersInvalidException e) {
      e.printStackTrace();
    }
  }
}
