package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.DAOException;
import fr.insalyon.creatis.gasw.execution.GaswMonitor;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KJob;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import io.kubernetes.client.openapi.ApiException;
import lombok.extern.log4j.Log4j;

@Log4j
final public class KMonitor extends GaswMonitor {

    final private List<KJob>    finishedJobs;
    private static KMonitor     instance;
    
    private boolean 		    stop;
    private KManager	        manager;

    public synchronized static KMonitor getInstance() {
        if (instance == null) {
            instance = new KMonitor();
            instance.start();
        }
        return instance;
    }

    public void setManager(KManager manager) { this.manager = manager; }

    private KMonitor() {
        super();
        finishedJobs = new ArrayList<>();
        stop = false;
    }

    private void statusChecker() {
        final ArrayList<KJob> jobs = manager.getUnfinishedJobs();

        for (final KJob j : jobs) {
            final GaswStatus stus = j.getStatus();

            if (stus != GaswStatus.RUNNING && stus != GaswStatus.QUEUED && stus != GaswStatus.UNDEFINED && stus != GaswStatus.NOT_SUBMITTED) {
                j.setTerminated(true);
                finishedJobs.add(j);
            } else if (stus ==  GaswStatus.RUNNING) {
                updateJob(j.getData().getJobID(), stus);
            }
        }
    }

    @Override
    public void run() {
        while (!stop) {
            statusChecker();
            try {
                while (hasFinishedJobs()) {
                    final KJob kJob = pullFinishedJobID();
                    final GaswStatus status = kJob.getStatus();
                    final Job job = jobDAO.getJobByID(kJob.getData().getJobID());
                    
                    if (status == GaswStatus.ERROR || status == GaswStatus.COMPLETED) {
                        job.setExitCode(kJob.getExitCode());
                        job.setStatus(job.getExitCode() == 0 ? GaswStatus.COMPLETED : GaswStatus.ERROR);
                    } else {
                        job.setStatus(status);
                    }
                    
                    jobDAO.update(job);
                    new KOutputParser(kJob).start();
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());

            } catch (GaswException | DAOException | InterruptedException e) {
                log.error(e);
            }
        }
    }

    @Override
    public synchronized void add(final String jobID, final String symbolicName, final String fileName, final String parameters) throws GaswException {
        final Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
            GaswStatus.QUEUED, symbolicName, fileName, parameters,
            KConstants.EXECUTOR_NAME);

        add(job);
        log.info("Adding job: " + jobID);
        try {
            job.setQueued(new Date());
            jobDAO.update(job);

        } catch (DAOException ex) {
            log.error("Failed to add the job", ex);
        }
    }

    public KJob pullFinishedJobID() {
        final KJob lastJob = finishedJobs.get(0);

        finishedJobs.remove(lastJob);
        return lastJob;
    }

    public boolean hasFinishedJobs() {
        return ! finishedJobs.isEmpty();
    }

    public synchronized void addFinishedJob(final KJob job) {
        finishedJobs.add(job);
    }

    public synchronized void finish() {
        if (instance != null) {
            log.info("Monitor is off !");
            instance.stop = true;
            instance = null;
        }
    }

    public void updateJob(final String jobID, final GaswStatus status) {
        try {
            final Job job = jobDAO.getJobByID(jobID);

            if (job.getStatus() != status) {
                job.setStatus(status);
                jobDAO.update(job);
            }
        } catch (DAOException e) {
            log.error(e);
        }
    }

    @Override
    protected void kill(final Job job) {
        final KJob kJob = manager.getJob(job.getId());

        if (kJob == null) {
            return ;
        }
        try {
            kJob.kill();
        } catch (ApiException e) {
            log.error("Failed to kill the job " + job.getId(), e);
        }
    }

    @Override
    protected void reschedule(Job job) {}

    @Override
    protected void replicate(Job job) {}

    @Override
    protected void killReplicas(Job job) {}

    @Override
    protected void resume(Job job) {}
}
