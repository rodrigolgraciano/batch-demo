package com.rh.batch.demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rh.batch.demo.domain.Rider;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Listener for the job. Can be used to explain jobs can have a parent
 * and inherit the listeners from it
 */
@Component
public class TourJobCompletionNotificationListener implements JobExecutionListener {

  private static final Logger log = LoggerFactory.getLogger( TourJobCompletionNotificationListener.class);

  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public TourJobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
      log.warn("!!! RIDER JOB FINISHED! Time to verify the results");

      jdbcTemplate.query("SELECT position, name, team, times FROM rider",
        (rs, row) -> new Rider(
          rs.getInt(1),
          rs.getString(2),
          rs.getString(3),
          rs.getString(4)
          )
      ).forEach(rider -> log.info("Found < {} > in the database.", rider));
    }
  }
}
