package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.KMonitor;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KVolumeData;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import lombok.extern.log4j.Log4j;

@Log4j
public class KManager {

    private String						workflowName;
    private KVolume 					workflowVolume;
    private List<KVolume>				customVolumes;
    private KConfig                     config;
    
    private volatile ArrayList<KJob> 	jobs;
    private Boolean						end;
    private Boolean						init;

    public KManager(String workflowName) {
        this.workflowName = workflowName;
        this.jobs = new ArrayList<KJob>();
        this.init = null;
        this.config = KConfiguration.getInstance().getConfig();
        this.customVolumes = new ArrayList<KVolume>();
    }

    public void init() {

        System.err.println("K8s Manager init with " + workflowName);
        try {
            checkNamespace();
            System.err.println("Namespaces checked !");
            checkAllVolumes();
            System.err.println("SharedUser volume checked !");
            checkOutputsDir();
            System.err.println("User ouputs directories checked !");

            init = true;
        } catch (Exception e) {
            log.error("Failed to init the manager", e);
            init = false;
        }
    }

    public void destroy() {
        end = true;

        try {
            if (this.workflowVolume != null)
                workflowVolume.deletePVC();
            if (this.workflowVolume != null)
                workflowVolume.deletePV();
            
            for (KJob job : jobs) {
                job.clean();
            }
            workflowVolume = null;
        } catch (ApiException e) {
            log.error("Failed to destroy the manager");
        }
    }

    /**
     * Check if the k8s cluster already have the namespace
     * if it isn't here, then it is created
     */
    public void checkNamespace() throws ApiException {
        CoreV1Api api = KConfiguration.getInstance().getCoreApi();
        V1NamespaceList list = api.listNamespace().execute();
        String targetName = config.getK8sNamespace();
        
        for (V1Namespace ns : list.getItems()) {
            String name = ns.getMetadata().getName();
            
            if (name.equals(targetName))
                return;
        }

        V1Namespace ns = new V1Namespace()
            .metadata(new V1ObjectMeta().name(targetName));

        api.createNamespace(ns).execute();
    }
    
    /**
     * Check all of the volumes presents inside the config
     */
    public void checkAllVolumes() throws ApiException {
        // workflow
        KVolumeData workflowVolumeData = new KVolumeData();

        workflowVolumeData.setName(workflowName);
        workflowVolumeData.setMountPathContainer(KConstants.workflowsLocation + workflowName);
        workflowVolumeData.setAccessModes("ReadWriteMany");
        workflowVolumeData.setNfsFolder(workflowName);

        workflowVolume = new KVolume(KConfiguration.getInstance(), workflowVolumeData);
        workflowVolume.createPV();
        workflowVolume.createPVC();

        // others
        for (KVolumeData configVolume : config.getVolumes()) {
            KVolume kVolume = KVolume.retrieve(configVolume);

            if (kVolume == null) {
                kVolume = new KVolume(KConfiguration.getInstance(), configVolume);

                kVolume.createPV();
                kVolume.createPVC();
            }
            customVolumes.add(kVolume);
        }
    }

    /**
     * Check for existance of STDOUR and STDERR from the plugin machine.
     */
    public void checkOutputsDir() {
        String[] dirs = { GaswConstants.OUT_ROOT, GaswConstants.ERR_ROOT, "./cache" };
        
        for (String dirName : dirs) {
            File dir = new File(dirName);

            if ( ! dir.exists())
                dir.mkdirs();
        }
    }

    /**
     * Check if the init state is finished
     */
    private boolean isReady() {
        int i = 0;

        while (true) {
            if (init != null && init == true) {
                return true;
            } else if (i < config.getOptions().getMaxRetryToPush() || (init != null && init == false)) {
                Utils.sleepNException(10000);
            } else {
                return false;
            }
            i++;
        }
    }

    /**
     * Create a thread instance that will launch job when ressources are available (volumes)
     * The end variable is used to know if a thread instance has already been launched.
     */
    private void submitter(KJob exec) {
        if (end == null) {
            end = false;
            new Thread(this.new KRunner()).start();
        }
        synchronized (this) {
            jobs.add(exec);
        }
    }

    /**
     * Public submitter that prepare the K8sJob object and wait for the manager to be initied (in case of slow cluster)
     */
    public void submitter(String cmd, String dockerImage, String jobID) {
        KJob exec = new KJob(jobID, workflowName);
        List<KVolume> volumes = new ArrayList<>();

        volumes.add(workflowVolume);
        volumes.addAll(customVolumes);
        if (isReady()) {
            exec.getData().setCommand(cmd);
            exec.getData().setImage(dockerImage);
            exec.getData().setVolumes(volumes);
            exec.getData().setWorkingDir(workflowVolume.getData().getMountPathContainer());
            exec.configure();
            submitter(exec);
        } else {
            KMonitor.getInstance().addFinishedJob(exec);
            exec.getData().setStatus(GaswStatus.STALLED);
        }
    }

    public ArrayList<KJob> getUnfinishedJobs() { 
        ArrayList<KJob> copy = new ArrayList<KJob>(jobs);

        System.err.println("[BEFORE-COPY] " + copy.toString());
        Iterator<KJob> it = copy.iterator();
        while (it.hasNext()) {
            if (it.next().isTerminated())
               it.remove();
        }
        return copy; 
    }

    public KJob getJob(String jobId) {
        return jobs.stream()
                .filter(job -> job.getData().getJobID().equals(jobId))
                .findFirst()
                .orElse(null);
    } 

    class KRunner implements Runnable {
        private Boolean ready = false;
        private DateTime startedTime;

        @Override
        public void run() {
            try {
                startedTime = DateTime.now();
                loop();
            } catch (GaswException e) {
                log.error(e.getMessage());
            } catch (Exception e) {
                log.error("Something bad happened during the K8sRunner", e);
            }
        }

        private void loop() throws ApiException, InterruptedException, GaswException {
            while (ready == false) {
                Duration diff = new Duration(startedTime, DateTime.now());
                
                if (diff.getStandardSeconds() > 120)
                    throw new GaswException("Volume wasn't eady in 2 minutes, aborting !");
                else {
                    checker();
                    Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
                }
            }
            while (end == false) {
                synchronized (this) {
                    for (KJob exec : jobs) {
                        if (exec.getStatus() == GaswStatus.NOT_SUBMITTED) {
                            exec.getData().setStatus(GaswStatus.QUEUED);
                            exec.start();
                        }
                    }
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            }
        }

        private synchronized void checker() {
            if (!ready && workflowVolume.isAvailable()) {
                for (KVolume custom : customVolumes) {
                    if (custom.isAvailable() ==  false)
                        break;
                }
                ready = true;
            }
        }
    }
}
