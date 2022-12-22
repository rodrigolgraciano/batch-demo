package com.rh.batch.demo.configuration;

import com.rh.batch.demo.domain.Race;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.RecordFieldSetMapper;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDateTime;

/**
 * Multistep job. Starting point to cover step-flow
 */
@Configuration
@Profile("multiStep")
public class RaceMultiStepJob {

	private static final Logger log = LoggerFactory.getLogger( RaceMultiStepJob.class );

	@Bean
	public Job multiStepJob(JobRepository jobRepository, Step raceMultiStep1, Step raceMultiStep2) {
		return new JobBuilder( "MultiStepJob", jobRepository )
				.incrementer( new RunIdIncrementer() )
				.start( raceMultiStep1 )
				.next( raceMultiStep2 )
				.build();
	}

	@Bean(name = "raceMultiStep1")
	public Step multiStep1(JobRepository jobRepository, PlatformTransactionManager transactionManager, @Qualifier("raceReader") FlatFileItemReader<Race> raceReader, JdbcBatchItemWriter<Race> raceWriter) {
		return new StepBuilder( "step1", jobRepository )
				.<Race, Race>chunk( 2, transactionManager )
				.reader( raceReader )
				.writer( raceWriter )
				.build();
	}

	@Bean(name = "raceReader")
	public FlatFileItemReader<Race> reader() {
		return new FlatFileItemReaderBuilder<Race>()
				.name( "simpleItemReader" )
				.resource( new ClassPathResource( "race1_results.csv" ) )
				.delimited()
				.names( "position", "pilot" )
				.fieldSetMapper( new RecordFieldSetMapper<>( Race.class ) )
				.build();
	}

	@Bean(name = "raceWriter")
	public JdbcBatchItemWriter<Race> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Race>()
				.sql( "INSERT INTO championship (position, pilot) VALUES (:position, :pilot)" )
				.itemSqlParameterSourceProvider( new BeanPropertyItemSqlParameterSourceProvider<>() )
				.dataSource( dataSource )
				.build();
	}

	@Bean(name = "raceMultiStep2")
	public Step multiStep2(JobRepository jobRepository, PlatformTransactionManager transactionManager, @Qualifier("step2Writer") FlatFileItemWriter<Race> step2Writer) {
		return new StepBuilder("multiStep2", jobRepository )
				.<Race, Race>chunk( 2, transactionManager )
				.reader( dbReader(null) )
				.writer( step2Writer )
				.build();
	}

	@Bean(name = "dbReader")
	public JdbcCursorItemReader dbReader(DataSource dataSource) {
		try {
			Thread.sleep( 5000 );
		}
		catch (InterruptedException e) {
			log.error( "Error reading from db" );
			e.printStackTrace();
		}
		return new JdbcCursorItemReaderBuilder<Race>()
				.name( "dbReader" )
				.dataSource( dataSource )
				.sql( "SELECT pilot, position FROM championship WHERE pilot LIKE 'C%'" )
				.rowMapper( new DataClassRowMapper<>( Race.class ) )
				.build();
	}

	@Bean(name = "step2Writer")
	public FlatFileItemWriter<Race> step2Writer() {

		return new FlatFileItemWriterBuilder<Race>()
				.name( "step2Writer" )
				.resource( new FileSystemResource( String.format( "./multiStep_%s.txt", LocalDateTime.now() ) ))
				.delimited()
				.fieldExtractor( new PassThroughFieldExtractor<>() )
				.build();
	}
}
