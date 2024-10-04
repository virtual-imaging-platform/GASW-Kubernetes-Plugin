package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.Date;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.DAOException;
import fr.insalyon.creatis.gasw.dao.DAOFactory;
import fr.insalyon.creatis.gasw.dao.JobDAO;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.execution.GaswSubmit;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import lombok.extern.log4j.Log4j;

@Log4j
public class KSubmit extends GaswSubmit {

    final private KManager 	    manager;

    public KSubmit(final GaswInput gaswInput, final KMinorStatusGenerator minorStatusServiceGenerator, final KManager manager) throws GaswException {
        super(gaswInput, minorStatusServiceGenerator);
        this.manager = manager;
        scriptName = generateScript();
    }

    @Override
    public String submit() throws GaswException {
        final String fileName = scriptName.substring(0, scriptName.lastIndexOf("."));
        final StringBuilder params = new StringBuilder();

        for (final String p : gaswInput.getParameters()) {
            params.append(p + " ");
        }

        KMonitor.getInstance().add(fileName, gaswInput.getExecutableName(), fileName, params.toString());
        wrappedSubmit(fileName);
        log.info("K8s Executor Job ID: " + fileName);

        return fileName;
    }

    private void wrappedSubmit(final String jobID) throws GaswException {
        try {
            // GAWS DAO
            final JobDAO jobDAO = DAOFactory.getDAOFactory().getJobDAO();
            final Job job = jobDAO.getJobByID(jobID);

            job.setStatus(GaswStatus.QUEUED);
            job.setDownload(new Date());
            jobDAO.update(job);

            // Kubernetes
            final String cmd = "bash " + GaswConstants.SCRIPT_ROOT + "/" + scriptName;
            manager.submitter(cmd, "ethaaalpha/podman-boutiques:latest", jobID);

        } catch (DAOException e) {
            log.error(e.getStackTrace());
            throw new GaswException("Failed to submit the job (wrapped command)");
        }
    }
}
