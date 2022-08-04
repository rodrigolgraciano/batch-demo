package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Race;
import com.rh.batch.demo.listener.JobCompletionNotificationListener;
import com.rh.batch.demo.processor.RaceItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.RecordFieldSetMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * RaceJob definition class. This job reads from file and writes to DB.
 * Introduces
 * <li>Chunk</li>
 * <li>Listeners - Job and Skip</li>
 * <li>Skip/Retry</li>
 */
@Configuration
public class RaceProcessorJob {
  private static final Logger log = LoggerFactory.getLogger(RaceProcessorJob.class);
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  public RaceProcessorJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  @Bean(name = "raceReader")
  public FlatFileItemReader<Race> raceReader() {
    return new FlatFileItemReaderBuilder<Race>()
      .name("simpleItemReader")
      .resource(new ClassPathResource("race1_results.csv"))
      .delimited()
      .names("position", "pilot")
      .fieldSetMapper(new RecordFieldSetMapper<>(Race.class))
      .build();
  }

  @Bean
  public RaceItemProcessor processor() {
    return new RaceItemProcessor();
  }

  @Bean(name = "importJob")
  public Job importUserJob(JobCompletionNotificationListener listener, @Qualifier("raceWriter") Step step1) {
    return jobBuilderFactory.get("importUserJob")
      .incrementer(new RunIdIncrementer())
      .listener(listener)
      .start(step1)
      .build();
  }

  @Bean(name = "raceWriter")
  public Step step1(JdbcBatchItemWriter<Race> multiWriter) {
    return stepBuilderFactory.get("step1")
      .<Race, Race>chunk(2)
      .reader(raceReader())
      .processor(processor())
      .writer(multiWriter)
      .faultTolerant()
      .skip(Exception.class).skipLimit(3)
      .retry(Exception.class).retryLimit(2)
      .listener(new SkipListener<Race, Race>() {
        @Override
        public void onSkipInRead(Throwable t) {
          log.warn("Skipped on read error");
        }

        @Override
        public void onSkipInWrite(Race item, Throwable t) {
          log.warn("Skipped on write error");
        }

        @Override
        public void onSkipInProcess(Race item, Throwable t) {
          log.warn("Skipped - Error processing result {}", item);
        }
      })
      .build();
  }


}
