package fr.insalyon.creatis.gasw.executor.kubernetes.config.json;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;

@Log4j @RequiredArgsConstructor
public class ConfigBuilder {
    
    final private String filePath;

    public KConfig get() {
        final ObjectMapper mapper = new ObjectMapper();

        try {

            return mapper.readValue(new File(filePath), KConfig.class);
        } catch (IOException  e) {
            log.error("Failed to read the configuration file", e);
            return null;
        }
    }
}
