package com.rh.batch.demo.configuration;

import javax.sql.DataSource;

import com.rh.batch.demo.domain.Rider;
import com.rh.batch.demo.listener.TourJobCompletionNotificationListener;
import com.rh.batch.demo.processor.RiderItemProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
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
public class TourProcessorJob {
	private static final Logger log = LoggerFactory.getLogger( TourProcessorJob.class );
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;

	public TourProcessorJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean(name = "importTourResults")
	public Job importResults(TourJobCompletionNotificationListener listener, @Qualifier("tourBasicStep") Step step) {
		return jobBuilderFactory.get( "importRaceResults" )
				.incrementer( new RunIdIncrementer() )
				.listener( listener )
				.start( step )
				.build();
	}

	@Bean(name = "tourBasicStep")
	public Step basicStep(JdbcBatchItemWriter<Rider> tourWriter) {
		return stepBuilderFactory.get( "tourBasicStep" )
				.<Rider, Rider>chunk( 10 )
				.reader( reader() )
				.processor( processor() )
				.writer( tourWriter )
				.build();
	}

	@Bean(name = "riderReader")
	public FlatFileItemReader<Rider> reader() {
		return new FlatFileItemReaderBuilder<Rider>()
				.name( "simpleItemReader" )
				.resource( new ClassPathResource( "tour_positions.csv" ) )
				.delimited()
				.names( "position", "name", "team", "times" )
				.fieldSetMapper( new RecordFieldSetMapper<>( Rider.class ) )
				.build();
	}

	@Bean
	public RiderItemProcessor processor() {
		return new RiderItemProcessor();
	}

	@Bean(name = "riderWriter")
	public JdbcBatchItemWriter<Rider> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Rider>()
				.sql( "INSERT INTO rider (position, name, team, times) VALUES (:position, :name, :team, :times)" )
				.itemSqlParameterSourceProvider( new BeanPropertyItemSqlParameterSourceProvider<>() )
				.dataSource( dataSource )
				.build();
	}

	@Bean(name = "importTourResultsFaultTolerant")
	public Job importResultsFaultTolerant(@Qualifier("tourFaultTolerantStep") Step step) {
		return jobBuilderFactory.get( "importRaceResults" )
				.incrementer( new RunIdIncrementer() )
				.start( step )
				.build();
	}

	@Bean(name = "riderReaderFT")
	public FlatFileItemReader<Rider> readerWithErrors() {
		return new FlatFileItemReaderBuilder<Rider>()
				.name( "simpleItemReader" )
				.resource( new ClassPathResource( "tour_positions_with_errors.csv" ) )
				.delimited()
				.names( "position", "name", "team", "times" )
				.fieldSetMapper( new RecordFieldSetMapper<>( Rider.class ) )
				.build();
	}

	@Bean(name = "tourFaultTolerantStep")
	public Step faultTolerantStep(JdbcBatchItemWriter<Rider> tourWriter) {
		return stepBuilderFactory.get( "faultTolerantStep" )
				.<Rider, Rider>chunk( 10 )
				.reader( readerWithErrors() )
				.processor( processor() )
				.writer( tourWriter )
				.faultTolerant()
				.skip( Exception.class ).skipLimit( 12 )
				.retry( Exception.class ).retryLimit( 2 )
				.listener( new SkipListener<>() {
					@Override
					public void onSkipInRead(Throwable t) {
						log.warn( "Skipped - Error reading: {}", t.getMessage() );
					}

					@Override
					public void onSkipInWrite(Rider rider, Throwable t) {
						log.warn( "Skipped - Error writing: {}, cause: {}", rider, t.getMessage() );
					}

					@Override
					public void onSkipInProcess(Rider rider, Throwable t) {
						log.warn( "Skipped - Error processing: {} , cause: {}", rider, t.getMessage() );
					}
				} )
				.build();
	}

}
