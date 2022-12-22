package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Race;
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
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.RecordFieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Explains the benefits and constraints of MultiFileReader
 * RecordFieldSetMapper -> automagically maps the data to a Record
 */
@Configuration
@Profile("multiFile")
public class RaceMultiFileJob {

  @Value("classpath:race*.csv")
  private Resource[] inputResources;

  @Bean(name = "importRaceMultiFileJob")
  public Job multiFileJob(JobRepository jobRepository, Step multiFileStep) {
    return new JobBuilder("multiFileJob", jobRepository)
      .incrementer(new RunIdIncrementer())
      .start(multiFileStep)
      .build();
  }

  @Bean
  public Step multiFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("multiFileStep", jobRepository)
      .<FieldSet, FieldSet>chunk(2,transactionManager)
      .reader(multiFileReader())
      .writer(raceDBWriter(null))
      .build();
  }

  @Bean
  public MultiResourceItemReader multiFileReader() {
    return new MultiResourceItemReaderBuilder<Race>()
      .delegate(raceFileReader())
      .name("multiFileReader")
      .resources(inputResources)
      .build();
  }

  @Bean
  public FlatFileItemReader<Race> raceFileReader() {
    return new FlatFileItemReaderBuilder<Race>()
      .name("simpleItemReader")
      .delimited()
      .names("position", "pilot")
      .fieldSetMapper(new RecordFieldSetMapper<>(Race.class))
      .build();
  }

  @Bean
  public JdbcBatchItemWriter<Race> raceDBWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Race>()
      .sql("INSERT INTO championship (position, pilot) VALUES (:position, :pilot)")
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
      .dataSource(dataSource)
      .build();
  }
}
