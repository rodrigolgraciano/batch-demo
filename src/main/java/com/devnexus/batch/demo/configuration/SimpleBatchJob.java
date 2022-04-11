package com.devnexus.batch.demo.configuration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class SimpleBatchJob {

  private static final Logger log = LoggerFactory.getLogger(SimpleBatchJob.class);
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;


  public SimpleBatchJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean
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

  @Bean(name = "simpleJob")
  public Job simpleJob(Step simpleStep) {
    return jobBuilderFactory.get("simpleJob")
      .incrementer(new RunIdIncrementer())
      .start(simpleStep)
      .build();
  }

  @Bean
  public Step simpleStep() {
    return stepBuilderFactory.get("simpleStep")
      .<FieldSet, FieldSet>chunk(2)
      .reader(simpleReader())
      .writer(simpleWriter())
      .build();
  }
}
