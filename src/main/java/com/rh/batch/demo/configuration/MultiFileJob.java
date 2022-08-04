package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Race;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
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
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

/**
 * Explains the benefits and constraints of MultiFileReader
 */
@Configuration
public class MultiFileJob {

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  @Value("classpath:race*.csv")
  private Resource[] inputResources;

  public MultiFileJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean(name = "multiJob")
  public Job simpleJob(Step multiFileStep) {
    return jobBuilderFactory.get("multiFileJob")
      .incrementer(new RunIdIncrementer())
      .start(multiFileStep)
      .build();
  }

  @Bean
  public MultiResourceItemReader multiFileReader() {
    return new MultiResourceItemReaderBuilder<Race>()
      .delegate(singleReader())
      .name("multiFileReader")
      .resources(inputResources)
      .build();
  }

  @Bean
  public FlatFileItemReader<Race> singleReader() {
    return new FlatFileItemReaderBuilder<Race>()
      .name("simpleItemReader")
      .delimited()
      .names("position", "pilot")
      .fieldSetMapper(new RecordFieldSetMapper<>(Race.class))
      .build();
  }


  @Bean
  public Step multiFileStep(JdbcBatchItemWriter<Race> writer) {
    return stepBuilderFactory.get("multiFileStep")
      .<FieldSet, FieldSet>chunk(2)
      .reader(multiFileReader())
      .writer(writer)
      .build();
  }

  @Bean
  public JdbcBatchItemWriter<Race> multiWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Race>()
      .sql("INSERT INTO championship (position, pilot) VALUES (:position, :pilot)")
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Race>())
      .dataSource(dataSource)
      .build();
  }
}
