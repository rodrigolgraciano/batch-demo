package com.rh.batch.demo.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * First demo. Introduces the concept of Job, Step, Reader, Writer
 * Reads from File and writer to File
 */
@Configuration
@Profile("simple")
public class SimpleBatchJob {

  @Bean(name = "simpleJob")
  public Job simpleJob(JobRepository jobRepository, Step step) {
    return new JobBuilder("simpleJob", jobRepository)
      .incrementer(new RunIdIncrementer())
      .start(step)
      .build();
  }

  @Bean
  public Step simpleStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("simpleStep", jobRepository)
      .<FieldSet, FieldSet>chunk(1, transactionManager)
      .reader(simpleReader())
      .writer(simpleWriter())
      .build();
  }

  @Bean(name = "simpleReader")
  public FlatFileItemReader<FieldSet> simpleReader() {
    return new FlatFileItemReaderBuilder<FieldSet>()
      .name("SimpleItemReader")
      .resource(new ClassPathResource("race1_results.csv"))
      .delimited()
      .names("position", "pilot")
      .fieldSetMapper(new PassThroughFieldSetMapper())
      .build();
  }

  @Bean(name = "simpleWriter")
  public FlatFileItemWriter<FieldSet> simpleWriter() {
    return new FlatFileItemWriterBuilder<FieldSet>()
      .name("simpleFileWriter")
      .resource(new FileSystemResource("/Users/graciano/workspace/batch-demo/results.txt"))
      .delimited()
      .fieldExtractor(new PassThroughFieldExtractor<>())
      .build();
  }
}
