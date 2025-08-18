package com.sync.demo;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(
        basePackages = "com.sync.demo",
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.REGEX,
                pattern = "pg\\.wiezon\\.com\\.biz\\..*"
        )
)
public class DemoApplication extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(DemoApplication.class);
    }

    public static void main(String[] args) {

        ConfigurableApplicationContext ctx = SpringApplication.run(DemoApplication.class, args);
        ServiceManager ac = new ServiceManager();
        ac.setApplicationContext(ctx);

    }


}
