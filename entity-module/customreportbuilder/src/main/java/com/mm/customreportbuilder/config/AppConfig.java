package com.mm.customreportbuilder.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ FactsProperties.class, DimsProperties.class })
public class AppConfig {}
