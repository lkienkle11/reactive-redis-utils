package com.microservices.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ReactiveRedisUtils {
    ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    ObjectMapper objectMapper;

    public ReactiveRedisUtils(ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    }

    public Mono<Boolean> hasKey(String key) {
        return reactiveRedisTemplate.hasKey(key);
    }

    public Mono<Object> getFromRedis(String key) {
        return reactiveRedisTemplate.opsForValue().get(key);
    }

    public <T> Mono<T> getFromRedis(String key, Class<T> clazz) {
        return reactiveRedisTemplate.opsForValue().get(key)
                .map(data -> objectMapper.convertValue(data, clazz));
    }

    public Mono<Void> saveToRedis(String key, Object value, long timeout, TimeUnit unit) {
        Duration ttl = Duration.ofMillis(unit.toMillis(timeout));
        return reactiveRedisTemplate
                .opsForValue()
                .set(key, value, ttl)
                .flatMap(saved -> {
                    if (!saved) {
                        return Mono.error(new IllegalStateException("Failed to save key: " + key));
                    }
                    return Mono.empty();
                });
    }

    public Flux<Object> getFromSet(String key) {
        return reactiveRedisTemplate
                .opsForSet()
                .members(key);
    }

    public <T> Flux<T> getFromSet(String key, Class<T> clazz) {
        return reactiveRedisTemplate
                .opsForSet()
                .members(key)
                .map(data -> objectMapper.convertValue(data, clazz));
    }

    public Mono<Long> saveToSet(String key, Object value, long timeout, TimeUnit unit) {
        Duration ttl = Duration.ofMillis(unit.toMillis(timeout));

        return reactiveRedisTemplate
                .opsForSet()
                .add(key, value)
                .flatMap(saved -> {
                    if (saved == 0L) {
                        return Mono.error(new IllegalStateException("Failed to add value to set: " + key));
                    }
                    return reactiveRedisTemplate
                            .expire(key, ttl)
                            .flatMap(expired -> {
                                if (!expired) {
                                    return Mono.error(new IllegalStateException("Failed to set TTL for key: " + key));
                                }
                                return Mono.just(saved);
                            });
                });
    }
}
