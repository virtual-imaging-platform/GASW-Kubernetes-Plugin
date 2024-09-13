package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.KMonitor;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import lombok.extern.log4j.Log4j;

@Log4j
public class KManager {

    private String						workflowName;
    private KVolume 					volume;
    private KVolume					    sharedVolume;
    
    private volatile ArrayList<KJob> 	jobs;
    private Boolean						end;
    private Boolean						init;

    public KManager(String workflowName) {
        this.workflowName = workflowName;
        this.jobs = new ArrayList<KJob>();
        this.init = null;
    }

    public void init() {
        KConfiguration conf = KConfiguration.getInstance();

        System.err.println("K8s Manager init with " + workflowName);
        try {
            checkNamespace();
            System.err.println("Namespaces checked !");
            checkSharedVolume();
            System.err.println("SharedUser volume checked !");
            checkOutputsDir();
            System.err.println("User ouputs directories checked !");

            volume = new KVolume(conf, workflowName, "ReadWriteMany");
            volume.createPV();
            volume.createPVC();
            System.err.println("Workflow volume created !");

            init = true;
        } catch (Exception e) {
            log.error("Failed to init the manager", e);
            init = false;
        }
    }

    public void destroy() {
        end = true;

        try {
            if (this.volume != null)
                volume.deletePVC();
            if (this.volume != null)
                volume.deletePV();
            
            for (KJob job : jobs) {
                job.clean();
            }
            volume = null;
        } catch (ApiException e) {
            log.error("Failed to destroy the manager");
        }
    }

    /**
     * Check if the k8s cluster already have the namespace
     * if it isn't here, then it is created
     */
    public void checkNamespace() throws ApiException {
        CoreV1Api api = KConfiguration.getInstance().getK8sCoreApi();
        V1NamespaceList list = api.listNamespace().execute();
        String targetName = KConfiguration.getInstance().getK8sNamespace();
        
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
     * Check if the /workflows/sharedata volume exist 
     */
    public void checkSharedVolume() throws ApiException {
        KVolume sharedUserVolume = KVolume.retrieve("SharedData", "ReadOnlyMany");

        if (sharedUserVolume == null) {
            sharedUserVolume = new KVolume(KConfiguration.getInstance(), "SharedData", "ReadOnlyMany");

            sharedUserVolume.createPV();
            sharedUserVolume.createPVC();
        }
        sharedVolume = sharedUserVolume;
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
            } else if (i < KConstants.maxRetryToPush || (init != null && init == false)) {
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
            new Thread(this.new K8sRunner()).start();
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

        if (isReady()) {
            exec.getData().setCommand(cmd);
            exec.getData().setImage(dockerImage);
            exec.getData().setVolumes(Arrays.asList(volume, sharedVolume));
            exec.getData().setWorkingDir(KConstants.mountPathContainer + volume.getName());
            exec.configure();
            submitter(exec);
        } else {
            KMonitor.getInstance().addFinishedJob(exec);
            exec.getData().setStatus(GaswStatus.STALLED);
        }
    }

    class K8sRunner implements Runnable {
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
            if (!ready && volume.isAvailable() && sharedVolume.isAvailable())
                ready = true;
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
        for (KJob j : jobs) {
            if (j.getData().getJobID() == jobId)
                return j;
        }
        return null;
    } 
}
