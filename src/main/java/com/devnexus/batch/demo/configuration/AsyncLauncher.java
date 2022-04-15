package com.devnexus.batch.demo.configuration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Demonstrates how to create an asyncJobLauncher
 */
@Configuration
public class AsyncLauncher {

  private JobRepository jobRepository;

  public AsyncLauncher(JobRepository jobRepository) {
    this.jobRepository = jobRepository;
  }

  @Bean(name = "asyncJobLauncher")
  public JobLauncher simpleJobLauncher() throws Exception {
    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }
}
