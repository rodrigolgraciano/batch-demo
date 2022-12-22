package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Rider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.RecordFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * RaceJob definition class. This job reads from file and writes to DB.
 * Introduces
 * <li>Listeners - Skip</li>
 * <li>FaultTolerance - Skip Limit</li>
 * <li>Skip/Retry</li>
 */
@Configuration
@Profile("tourFault")
public class TourFaultTolerantJob {
  private static final Logger log = LoggerFactory.getLogger(TourFaultTolerantJob.class);

  @Bean
  public Job importResultsFaultTolerant(JobRepository jobRepository, Step step) {
    return new JobBuilder("importRaceResults", jobRepository)
      .incrementer(new RunIdIncrementer())
      .start(step)
      .build();
  }

  @Bean
  public Step faultTolerantStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("faultTolerantStep", jobRepository)
      .<Rider, Rider>chunk(100, transactionManager)
      .reader(readerWithErrors())
      .writer(writer(null))
      .faultTolerant()
      .skip(Exception.class).skipLimit(12)
      .retry(Exception.class).retryLimit(2)
      .listener(new SkipListener<>() {
        @Override
        public void onSkipInRead(Throwable t) {
          log.warn("Skipped - Error reading: {}", t.getMessage());
        }

        @Override
        public void onSkipInWrite(Rider rider, Throwable t) {
          log.warn("Skipped - Error writing: {}, cause: {}", rider, t.getMessage());
        }

        @Override
        public void onSkipInProcess(Rider rider, Throwable t) {
          log.warn("Skipped - Error processing: {} , cause: {}", rider, t.getMessage());
        }
      })
      .build();
  }

  @Bean
  public JdbcBatchItemWriter<Rider> writer(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Rider>()
      .sql("INSERT INTO rider (position, name, team, times) VALUES (:position, :name, :team, :times)")
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
      .dataSource(dataSource)
      .build();
  }

  @Bean(name = "riderReaderFT")
  public FlatFileItemReader<Rider> readerWithErrors() {
    return new FlatFileItemReaderBuilder<Rider>()
      .name("simpleItemReader")
      .resource(new ClassPathResource("tour_positions_with_errors.csv"))
      .delimited()
      .names("position", "name", "team", "times")
      .fieldSetMapper(new RecordFieldSetMapper<>(Rider.class))
      .build();
  }
}
