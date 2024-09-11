package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.K8sMonitor;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConstants;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");

    private String						workflowName;
    private K8sVolume 					volume;
    private K8sVolume					sharedVolume;
    
    private volatile ArrayList<K8sJob> 	jobs;
    private Boolean						end;
    private Boolean						init;

    public K8sManager(String workflowName) {
        this.workflowName = workflowName;
        this.jobs = new ArrayList<K8sJob>();
        this.init = false;
    }

    public void init() {
        K8sConfiguration conf = K8sConfiguration.getInstance();
        System.err.println("K8s Manager init with " + workflowName);
        try {
            checkNamespace();
            System.err.println("Namespaces checked !");
            checkSharedVolume();
            System.err.println("SharedUser volume checked !");
            checkOutputsDir();
            System.err.println("User ouputs directories checked !");

            volume = new K8sVolume(conf, workflowName, "ReadWriteMany");
            volume.createPV();
            volume.createPVC();
            System.err.println("Workflow volume created !");

            init = true;
        } catch (Exception e) {
            logger.error("Failed to init the manager", e);
        }
    }

    public void destroy() {
        end = true;

        try {
            if (this.volume != null)
                volume.deletePVC();
            if (this.volume != null)
                volume.deletePV();
            
            /* hard cleaning not prod */
            for (K8sJob job : jobs) { job.clean(); }
            volume = null;
        } catch (ApiException e) {
            logger.error("Failed to destroy the manager");
        }
    }

    /*
     * Check if the k8s cluster already have the namespace
     * if it isn't here, then it is created
     */
    public void checkNamespace() throws ApiException {
        CoreV1Api api = K8sConfiguration.getInstance().getK8sCoreApi();
        V1NamespaceList list = api.listNamespace().execute();
        String targetName = K8sConfiguration.getInstance().getK8sNamespace();
        
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
        K8sVolume sharedUserVolume = K8sVolume.retrieve("SharedData", "ReadOnlyMany");

        if (sharedUserVolume == null) {
            sharedUserVolume = new K8sVolume(K8sConfiguration.getInstance(), "SharedData", "ReadOnlyMany");

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
     * Create a thread instance that will launch job when ressources are available (volumes)
     * The end variable is used to know if a thread instance has already been launched.
     * @param exec
     */
    private void submitter(K8sJob exec) {
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
        int i = 0;
        K8sJob exec = new K8sJob(jobID, workflowName);

        while (true) {
            if (init == true) {
                break;
            } else if (i == K8sConstants.maxRetryToPush) {
                synchronized (this) {
                    K8sMonitor.getInstance().addFinishedJob(exec);
                }
                exec.setStatus(GaswStatus.STALLED);
                return ;
            } else {
                try {
                    System.err.println("[ATTENTE] Manager pas encore Init");
                    TimeUnit.MILLISECONDS.sleep(10000);
                } catch (InterruptedException e) {}
            }
            i++;
        }

        exec.setCommand(cmd);
        exec.setImage(dockerImage);
        exec.setVolumes(Arrays.asList(volume, sharedVolume));
        exec.setWorkingDir(K8sConstants.mountPathContainer + volume.getName());
        exec.configure();
        submitter(exec);
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
                logger.error(e.getMessage());
            } catch (Exception e) {
                logger.error("Something bad happened during the K8sRunner", e);
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
                    for (K8sJob exec : jobs) {
                        if (exec.getStatus() == GaswStatus.NOT_SUBMITTED) {
                            exec.setStatus(GaswStatus.QUEUED);
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

    public ArrayList<K8sJob> getUnfinishedJobs() { 
        ArrayList<K8sJob> copy = new ArrayList<K8sJob>(jobs);

        System.err.println("[BEFORE-COPY] " + copy.toString());
        Iterator<K8sJob> it = copy.iterator();
        while (it.hasNext()) {
            if (it.next().isTerminated())
               it.remove();
        }
        return copy; 
    }

    public K8sJob getJob(String jobId) {
        for (K8sJob j : jobs) {
            if (j.getJobID() == jobId)
                return j;
        }
        return null;
    } 
}
