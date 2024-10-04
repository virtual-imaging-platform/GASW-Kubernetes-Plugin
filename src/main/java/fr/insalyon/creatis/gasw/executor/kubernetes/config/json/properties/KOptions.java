package fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor
public class KOptions {
    
    @JsonProperty("ttlJob") 
    private int ttlJob = 500;
    
    @JsonProperty("statusRetry")
    private int statusRetry = 5;
    
    @JsonProperty("statusRetryWait")
    private int statusRetryWait = 2000;

    @JsonProperty("maxRetryToPush")
    private int maxRetryToPush = 5;

    @JsonProperty("timeToVolumeBeReady")
    private int timeToVolumeBeReady = 120;

    @JsonProperty("storageClassName")
    private String storageClassName = "";
}
