package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;

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
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.K8sStatus;

public class K8sMonitor extends GaswMonitor {

	private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
	private static K8sMonitor 	instance;
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
		stop = false;
	}

	private void statusChecker() {
		ArrayList<K8sJob> jobs = manager.getUnfinishedJobs();

		for (K8sJob j : jobs) {
			if (j.getStatus() == K8sStatus.FINISHED || j.getStatus() == K8sStatus.FAILED) {
				Integer exitCode = j.getExitCode();
				String jobId = j.getJobID();
				j.setTerminated();
				K8sSubmit.addFinishedJob(jobId, exitCode);
			}
		}
	}

	@Override
	public void run() {

		while (!stop) {
			statusChecker();
			try {
				while (K8sSubmit.hasFinishedJobs()) {

					String[] s = K8sSubmit.pullFinishedJobID().split("--");
					Job job = jobDAO.getJobByID(s[0]);
					job.setExitCode(Integer.parseInt(s[1]));

					if (job.getExitCode() == 0) {
						job.setStatus(GaswStatus.COMPLETED);
					} else {
						job.setStatus(GaswStatus.ERROR);
					}
					jobDAO.update(job);
					new K8sOutputParser(job.getId()).start();
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

		logger.info("Adding job: " + jobID);
		Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
				GaswStatus.QUEUED, symbolicName, fileName, parameters,
				K8sConstants.EXECUTOR_NAME);
		add(job);

		// Queued Time
		try {
			job.setQueued(new Date());
			jobDAO.update(job);
		} catch (DAOException ex) {
			// do nothing
		}
	}

	public synchronized void terminate() {
		stop = true;
		instance = null;
	}

	public static void finish() {
		if (instance != null) {
			instance.terminate();
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
