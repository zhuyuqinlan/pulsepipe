package com.pulsepipe.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Springdoc / Knife4j 接口文档配置，根据 springdoc.info 配置项生成 OpenAPI Info 信息。
 */
@Configuration
@EnableConfigurationProperties(SpringDocProperties.class)
public class SpringdocConfig {

    @Bean
    public OpenAPI openApi(SpringDocProperties properties) {
        SpringDocProperties.Contact contact = properties.getContact();
        SpringDocProperties.License license = properties.getLicense();

        Info info = new Info()
                .title(properties.getTitle())
                .description(properties.getDescription())
                .version(properties.getVersion())
                .termsOfService(properties.getTermsOfService())
                .summary(properties.getSummary());
        if (contact != null) {
            info.contact(new Contact()
                    .name(contact.getName())
                    .url(contact.getUrl())
                    .email(contact.getEmail()));
        }
        if (license != null) {
            info.license(new License()
                    .name(license.getName())
                    .url(license.getUrl()));
        }
        return new OpenAPI().info(info);
    }
}
