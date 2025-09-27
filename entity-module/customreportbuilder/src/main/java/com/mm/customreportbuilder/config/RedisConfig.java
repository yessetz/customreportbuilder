package com.mm.customreportbuilder.config;

import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

@Configuration
public class RedisConfig {
    @Value("${REDIS_HOST:localhost}")
    private String host;

    @Value("${REDIS_PORT:6379}")
    private int port;

    @Value("${REDIS_SSL:false}")
    private boolean ssl;

    @Value("${REDIS_USERNAME:}")
    private String username;

    @Value("${REDIS_PASSWORD:}")
    private String password;

    @Value("${REDIS_TIMEOUT_MS:3000}")
    private int timeoutMs;

    @Bean(destroyMethod = "shutdown")
    DefaultClientResources lettuceClientResources() {
        return DefaultClientResources.create();
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory(DefaultClientResources resources) {
        RedisStandaloneConfiguration standalone = new RedisStandaloneConfiguration(host, port);
        if (!username.isBlank()) {
            standalone.setUsername(username);
        }
        if (!password.isBlank()) {
            standalone.setPassword(RedisPassword.of(password));
        }

        LettuceClientConfiguration.LettuceClientConfigurationBuilder b = LettuceClientConfiguration.builder()
                .clientResources(resources)
                .commandTimeout(Duration.ofMillis(timeoutMs));

        if (ssl) {
            b.useSsl();
        }

        return new LettuceConnectionFactory(standalone, b.build());
    }

    @Bean
    public RedisTemplate<String, byte[]> redisBytesTemplate(LettuceConnectionFactory connectionFactory) {
        RedisTemplate<String, byte[]> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(org.springframework.data.redis.serializer.RedisSerializer.byteArray());
        template.afterPropertiesSet();
        return template;
    }

    @Bean("redisStringTemplate")
    public RedisTemplate<String, String> redisStringTemplate(LettuceConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}
