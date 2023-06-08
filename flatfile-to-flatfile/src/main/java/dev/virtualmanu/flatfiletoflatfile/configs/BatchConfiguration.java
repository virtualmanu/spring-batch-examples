package dev.virtualmanu.flatfiletoflatfile.configs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfiguration {

    @Value("${input.filepath}")
    private String inputFIlePath;

    @Value("${output.filepath}")
    private String outputFIlePath;

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder("job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step", jobRepository)
                .<FieldSet, FieldSet>chunk(1, transactionManager)
                .reader(flatFileReader())
                .writer(flatFileWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<FieldSet> flatFileReader() {
        return new FlatFileItemReaderBuilder<FieldSet>()
                .name("flatFileReader")
                .resource(new ClassPathResource(inputFIlePath))
                .delimited()
                .names("id", "name")
                .fieldSetMapper(new PassThroughFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public FlatFileItemWriter<FieldSet> flatFileWriter(){
        return new FlatFileItemWriterBuilder<FieldSet>()
                .name("flatFileWriter")
                .resource(new FileSystemResource(outputFIlePath))
                .delimited()
                .fieldExtractor(new PassThroughFieldExtractor<>())
                .build();
    }

}
