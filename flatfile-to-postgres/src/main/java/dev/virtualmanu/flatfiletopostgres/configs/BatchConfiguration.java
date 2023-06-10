package dev.virtualmanu.flatfiletopostgres.configs;

import dev.virtualmanu.flatfiletopostgres.models.Superhero;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.RecordFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {

    @Value("${input.filepath}")
    private String inputFIlePath;

    @Bean
    public Job job(JobRepository jobRepository, Step step){
        return new JobBuilder("job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new StepBuilder("step",jobRepository)
                .<Superhero, Superhero>chunk(1,platformTransactionManager)
                .reader(flatFileReader())
                .writer(jdbcWriter(null))
                .build();
    }

    @Bean
    public FlatFileItemReader<Superhero> flatFileReader() {
        return new FlatFileItemReaderBuilder<Superhero>()
                .name("flatFileReader")
                .resource(new ClassPathResource(inputFIlePath))
                .delimited()
                .names("id", "name")
                .fieldSetMapper(new RecordFieldSetMapper<>(Superhero.class))
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Superhero> jdbcWriter(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Superhero>()
                .sql("INSERT INTO squad(id,name) VALUES(:id,:name)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .dataSource(dataSource)
                .build();
    }
}
