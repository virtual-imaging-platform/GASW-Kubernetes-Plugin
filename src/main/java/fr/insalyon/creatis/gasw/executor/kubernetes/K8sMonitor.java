package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.DAOException;
import fr.insalyon.creatis.gasw.execution.GaswMonitor;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.K8sJob;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.K8sManager;

public class K8sMonitor extends GaswMonitor {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private static K8sMonitor 	instance;
    
    private List<K8sJob>        finishedJobs;
    private boolean 			stop;
    
    private K8sManager			manager;

    public synchronized static K8sMonitor getInstance() {
        if (instance == null) {
            instance = new K8sMonitor();
            instance.start();
        }
        return instance;
    }

    public void setManager(K8sManager manager) { this.manager = manager; }

    private K8sMonitor() {
        super();
        finishedJobs = new ArrayList<K8sJob>();
        stop = false;
    }

    private void statusChecker() {
        ArrayList<K8sJob> jobs = manager.getUnfinishedJobs();

        for (K8sJob j : jobs) {
            GaswStatus stus = j.getStatus();

            System.err.println("job : " + j.getJobID() + " : " + stus.toString());
            if (stus != GaswStatus.RUNNING && stus != GaswStatus.QUEUED && stus != GaswStatus.UNDEFINED && stus != GaswStatus.NOT_SUBMITTED) {
                j.setTerminated();
                finishedJobs.add(j);
            } else if (stus ==  GaswStatus.RUNNING) {
                updateJob(j.getJobID(), stus);
            }
        }
    }

    @Override
    public void run() {
        while (!stop) {
            System.err.println("je fais le check des jobs en cours !");
            statusChecker();
            try {
                while (hasFinishedJobs()) {
                    K8sJob kJob = pullFinishedJobID();
                    GaswStatus status = kJob.getStatus();
                    Job job = jobDAO.getJobByID(kJob.getJobID());
                    
                    if (status == GaswStatus.ERROR || status == GaswStatus.COMPLETED) {
                        job.setExitCode(kJob.getExitCode());
                        job.setStatus(job.getExitCode() == 0 ? GaswStatus.COMPLETED : GaswStatus.ERROR);
                    } else {
                        job.setStatus(status);
                    }
                    System.err.println("job : " + kJob.getJobID() + " final : " + job.getStatus());
                    
                    jobDAO.update(job);
                    new K8sOutputParser(job.getId(), manager).start();
                }

                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());

            } catch (GaswException ex) {
            } catch (DAOException ex) {
                logger.error(ex);
            } catch (InterruptedException ex) {
                logger.error(ex);
            }
        }
    }

    @Override
    public synchronized void add(String jobID, String symbolicName, String fileName, String parameters) throws GaswException {
        Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
            GaswStatus.QUEUED, symbolicName, fileName, parameters,
            K8sConstants.EXECUTOR_NAME);

        add(job);
        logger.info("Adding job: " + jobID);
        try {
            job.setQueued(new Date());
            jobDAO.update(job);

        } catch (DAOException ex) {
            System.err.println(ex.getMessage());
        }
    }

    public K8sJob pullFinishedJobID() {
        K8sJob lastJob = finishedJobs.get(0);

        finishedJobs.remove(lastJob);
        return lastJob;
    }

    public boolean hasFinishedJobs() {
        return ! finishedJobs.isEmpty();
    }

    public void addFinishedJob(K8sJob job) {
        finishedJobs.add(job);
    }

    public synchronized void finish() {
        if (instance != null) {
            System.err.println("Monitor is off !");
            instance.stop = true;
            instance = null;
        }
    }

    public void updateJob(String jobID, GaswStatus status) {
        try {
            var job = jobDAO.getJobByID(jobID);

            if (job.getStatus() != status) {
                job.setStatus(status);
                jobDAO.update(job);
                System.err.println("je viens de mettre à jour le job " + job.getId() + " sur le statut " + status.toString());
            }
        } catch (DAOException e) {
            System.err.println("ICI j'ai une dao exeception! " + e.getMessage());
        }
    }

    @Override
    protected void kill(Job job) {}

    @Override
    protected void reschedule(Job job) {}

    @Override
    protected void replicate(Job job) {}

    @Override
    protected void killReplicas(Job job) {}

    @Override
    protected void resume(Job job) {}
}
