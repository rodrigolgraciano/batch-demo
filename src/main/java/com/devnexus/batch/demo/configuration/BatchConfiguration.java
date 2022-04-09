package com.devnexus.batch.demo.configuration;

import com.devnexus.batch.demo.PersonReader;
import com.devnexus.batch.demo.domain.Person;
import com.devnexus.batch.demo.listener.JobCompletionNotificationListener;
import com.devnexus.batch.demo.processor.PersonItemProcessor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.util.List;

@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfiguration {
  public BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);


  @Bean
  public FlatFileItemReader<Person> reader() {
    return new FlatFileItemReaderBuilder<Person>()
      .name("personItemReader")
      .resource(new ClassPathResource("sample.csv"))
      .delimited()
      .names(new String[]{"firstName", "lastName"})
      .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
        setTargetType(Person.class);
      }})
      .build();
  }

  @Bean
  public PersonReader personReader() {
    PersonReader reader = new PersonReader();
    reader.setResource(new ClassPathResource("sample.csv"));
    BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
    fieldSetMapper.setTargetType(Person.class);
    DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
    DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
    delimitedLineTokenizer.setNames(new String[]{"firstName", "lastName"});
    lineMapper.setLineTokenizer(delimitedLineTokenizer);
    lineMapper.setFieldSetMapper(fieldSetMapper);
    reader.setLineMapper(lineMapper);
    reader.open(new ExecutionContext());
    return reader;
  }

  @Bean
  public PersonItemProcessor processor() {
    return new PersonItemProcessor();
  }

  @Bean
  public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Person>()
      .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
      .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
      .dataSource(dataSource)
      .build();
  }

  @Bean(name = "importJob")
  public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
    return jobBuilderFactory.get("importUserJob")
      .incrementer(new RunIdIncrementer())
      .listener(listener)
      .flow(step1)
      .end()
      .build();
  }

  @Bean
  public Step step1(JdbcBatchItemWriter<Person> writer) {
    return stepBuilderFactory.get("step1")
      .<Person, Person>chunk(2)
      .reader(personReader())
      .processor(processor())
      .writer(writer)
      .faultTolerant()
      .skip(Exception.class)
      .skipLimit(3)
      .listener(new SkipListener<Person, Person>() {
        @Override
        public void onSkipInRead(Throwable t) {
          log.warn("Skipped on read error");
        }

        @Override
        public void onSkipInWrite(Person item, Throwable t) {
          log.warn("Skipped on write error");

        }

        @Override
        public void onSkipInProcess(Person item, Throwable t) {
          log.warn("Skipped on process error");

        }
      })
      .listener(writeListener())
      .build();
  }

  @Bean
  public ItemWriteListener<Person> writeListener() {
    return new ItemWriteListener<>() {
      @Override
      public void beforeWrite(List<? extends Person> list) {

      }

      @Override
      public void afterWrite(List<? extends Person> list) {

      }

      @Override
      public void onWriteError(Exception e, List<? extends Person> list) {
        log.error("Error while inserting person " + list);
      }
    };
  }
}