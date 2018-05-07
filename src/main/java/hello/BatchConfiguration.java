package hello;

import java.util.Date;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableBatchProcessing
@Import({ BatchScheduler.class })
public class BatchConfiguration {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	private SimpleJobLauncher jobLauncher;

	@Autowired
	DataSource dataSource;

	@Scheduled(fixedRate = 50000)
	public void processBatch() throws Exception {

		log.info("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder()
				.addString("importUserJob", String.valueOf(System.currentTimeMillis())).toJobParameters();

		JobExecution execution = jobLauncher.run(importUserJob(), param);

		log.info("Job finished with status :" + execution.getStatus());
	}

	@Bean
	public PersonItemProcessor processor() {
		log.info("##### processor() #####");
		return new PersonItemProcessor();
	}

	// tag::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Person> reader() {
		log.info("##### reader() #####");
		return new FlatFileItemReaderBuilder<Person>().name("personItemReader")
				.resource(new ClassPathResource("sample-data.csv")).delimited()
				.names(new String[] { "firstName", "lastName" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				}).build();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer() {
		log.info("##### writer() #####");
		return new JdbcBatchItemWriterBuilder<Person>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)").dataSource(dataSource)
				.build();
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob() {

		log.info("##### importUserJob() #####");
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(step1())
				.end()
				.build();
	}

	@Bean
	public Step step1() {
		log.info("##### step1() #####");
		return stepBuilderFactory.get("step1").<Person, Person>chunk(2).reader(reader()).processor(processor())
				.writer(writer()).build();
	}

	

	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionNotificationListener();
	}


	// end::jobstep[]
}
