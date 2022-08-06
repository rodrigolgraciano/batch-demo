package com.rh.batch.demo.configuration;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Demonstrates how to create an asyncJobLauncher
 */
@Configuration
public class AsyncLauncher {

  private final JobRepository jobRepository;

  public AsyncLauncher(JobRepository jobRepository) {
    this.jobRepository = jobRepository;
  }

  @Bean(name = "asyncJobLauncher")
  public JobLauncher simpleJobLauncher() throws Exception {
    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.setTaskExecutor(taskExecutor());
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  @Bean
  TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor t = new ThreadPoolTaskExecutor();
    t.setCorePoolSize(10);
    t.setMaxPoolSize(100);
    t.setQueueCapacity(50);
    t.setAllowCoreThreadTimeOut(true);
    t.setKeepAliveSeconds(120);
    return t;
  }
}
