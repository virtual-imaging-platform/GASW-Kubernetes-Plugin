package fr.insalyon.creatis.gasw.executor.kubernetes.config.json;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
import lombok.extern.log4j.Log4j;

@Log4j
public class ConfigBuilder {
    
    private String filePath;

    public ConfigBuilder(String filePath) {
        this.filePath = filePath;
    }

    public KConfig get() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            KConfig loadedConfig = mapper.readValue(new File(filePath), KConfig.class);

            return loadedConfig;
        } catch (Exception e) {
            log.error("Failed to read the configuration file", e);
            return null;
        }
    }
}
