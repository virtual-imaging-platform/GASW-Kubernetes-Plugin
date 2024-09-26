package fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter @Setter @Accessors(chain = true)
public class KVolumeData {

    @JsonProperty(value = "name", required = true)
    private String name;

    @JsonProperty(value = "nfsFolder", required = true)
    private String nfsFolder;

    @JsonProperty(value = "mountPathContainer", required = true)
    private String mountPathContainer;

    @JsonProperty(value = "accessModes", required = true)
    private String accessModes;
}
