package com.mm.customreportbuilder.dims;

import com.mm.customreportbuilder.config.DimsProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

@Component
public class DimsRegistry {

    private final Map<String, DimsProperties.DimConfig> defs;

    public DimsRegistry(DimsProperties props) {
        this.defs = props.getDims();
    }

    public Set<String> names() {
        return defs.keySet();
    }

    public String readSql(String name) {
        DimsProperties.DimConfig cfg = defs.get(name);
        if (cfg == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown dim: " + name);
        }
        String path = cfg.getPath();
        String cp = path.startsWith("classpath:") ? path.substring("classpath:".length()) : path;
        try (InputStream in = new ClassPathResource(cp).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Cannot read SQL for dim: " + name, e);
        }
    }

    public Integer ttlSeconds(String name) {
        DimsProperties.DimConfig cfg = defs.get(name);
        return cfg != null ? cfg.getTtlSeconds() : null;
    }
}
