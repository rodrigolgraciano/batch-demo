package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Rider;
import com.rh.batch.demo.listener.TourJobCompletionNotificationListener;
import com.rh.batch.demo.processor.RiderItemProcessor;
import org.springframework.batch.core.Job;
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
 * <li>Chunk</li>
 * <li>Listeners</li>
 */
@Configuration
@Profile("tour")
public class TourProcessorJob {

  @Bean(name = "importTourResults")
  public Job importResults(TourJobCompletionNotificationListener listener, Step step, JobRepository jobRepository) {
    return new JobBuilder("importRaceResults", jobRepository)
      .incrementer(new RunIdIncrementer())
      .listener(listener)
      .start(step)
      .build();
  }

  @Bean(name = "tourBasicStep")
  public Step basicStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Rider> tourWriter) {
    return new StepBuilder("tourBasicStep", jobRepository)
      .<Rider, Rider>chunk(10, transactionManager)
      .reader(reader())
      .processor(processor())
      .writer(tourWriter)
      .build();
  }

  @Bean
  public FlatFileItemReader<Rider> reader() {
    return new FlatFileItemReaderBuilder<Rider>()
      .name("simpleItemReader")
      .resource(new ClassPathResource("tour_positions.csv"))
      .delimited()
      .names("position", "name", "team", "times")
      .fieldSetMapper(new RecordFieldSetMapper<>(Rider.class))
      .build();
  }

  @Bean
  public RiderItemProcessor processor() {
    return new RiderItemProcessor();
  }

  @Bean
  public JdbcBatchItemWriter<Rider> writer(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Rider>()
      .sql("INSERT INTO rider (position, name, team, times) VALUES (:position, :name, :team, :times)")
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
      .dataSource(dataSource)
      .build();
  }
}
