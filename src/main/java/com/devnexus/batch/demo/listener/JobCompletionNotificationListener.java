package com.devnexus.batch.demo.listener;


import com.devnexus.batch.demo.domain.Race;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Listener for the job. Can be used to explain jobs can have a parent
 * and inherit the listeners from it
 */
@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

  private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
      log.warn("!!! JOB FINISHED! Time to verify the results");

      jdbcTemplate.query("SELECT position , pilot FROM championship",
        (rs, row) -> new Race(
          rs.getInt(1),
          rs.getString(2))
      ).forEach(race -> log.info("Found <" + race + "> in the database."));
    }
  }
}