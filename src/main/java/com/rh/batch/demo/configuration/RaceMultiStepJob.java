package com.rh.batch.demo.configuration;

import java.time.LocalDateTime;

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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;

import javax.sql.DataSource;

/**
 * Multistep job. Starting point to cover step-flow
 */
@Configuration
public class RaceMultiStepJob {

	private static final Logger log = LoggerFactory.getLogger( RaceMultiStepJob.class );
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final DataSource dataSource;

	public RaceMultiStepJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
	}

	@Bean(name = "importRaceMultiStepJob")
	public Job multiStepJob(@Qualifier("raceMultiStep1") Step raceMultiStep1, Step raceMultiStep2) {
		return jobBuilderFactory
				.get( "MultiStepJob" )
				.incrementer( new RunIdIncrementer() )
				.start( raceMultiStep1 )
				.next( raceMultiStep2 )
				.build();
	}

	@Bean(name = "raceMultiStep1")
	public Step multiStep1(@Qualifier("raceReader") FlatFileItemReader<Race> raceReader, JdbcBatchItemWriter<Race> raceWriter) {
		return stepBuilderFactory.get( "step1" )
				.<Race, Race>chunk( 2 )
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
	public Step multiStep2(JdbcCursorItemReader dbReader, @Qualifier("step2Writer") FlatFileItemWriter<Race> step2Writer) {
		return stepBuilderFactory.get( "multiStep2" )
				.<Race, Race>chunk( 2 )
				.reader( dbReader )
				.writer( step2Writer )
				.build();
	}

	@Bean(name = "dbReader")
	public JdbcCursorItemReader dbReader() {
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
