package com.spring.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import com.spring.batch.model.Person;

@Configuration
@EnableBatchProcessing
public class SpringConfiguration {
	
	@Autowired
	public JobBuilderFactory  jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	

	@Bean
	public JdbcCursorItemReader<Person> reader(){
		JdbcCursorItemReader<Person> readPerson = new JdbcCursorItemReader<Person>();
		readPerson.setDataSource(dataSource);
		readPerson.setSql("SELECT person_id, first_name,last_name,email,age FROM person;");
		readPerson.setRowMapper(new PersonRowMapper());
		return readPerson;
	}

	@Bean
	public FlatFileItemWriter<Person> writer(){
		FlatFileItemWriter<Person> personWriter = new FlatFileItemWriter<Person>();
		personWriter.setResource(new ClassPathResource("persons.csv"));
		personWriter.setLineAggregator(new DelimitedLineAggregator<Person>() {{
			setDelimiter(",");
			setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {{
				setNames(new String[] {"id","firstName","lastName","email","age"});				
			}});			
		}});
		return personWriter;
	}
	
	@Bean
    public FlatFileItemReader<Person> reader2() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("persons.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] {"id","firstName", "lastName","email","age" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
     }
		
	 @Bean
	    public JdbcBatchItemWriter<Person> writerdb2() {
	        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
	        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
	        writer.setSql("INSERT INTO person2 (person_id,first_name, last_name,email,age) VALUES (:id,:firstName, :lastName,:email,:age)");
	        writer.setDataSource(dataSource);
	        return writer;
	    }

		
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Person,Person> chunk(100).reader(reader()).writer(writer()).build();
	}
	
	public Step step2() {
		return stepBuilderFactory.get("step2").<Person,Person> chunk(100).reader(reader2()).writer(writerdb2()).build();

	}
	
	@Bean
	public Job exportPersonJob(com.spring.batch.listener.JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("exportPersonJob")
				.incrementer(new RunIdIncrementer()).listener(listener).flow(step1()).next(step2()).end().build();				
	}
}
