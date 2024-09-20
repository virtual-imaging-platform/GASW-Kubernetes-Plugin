package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.io.File;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswExitCode;
import fr.insalyon.creatis.gasw.GaswOutput;
import fr.insalyon.creatis.gasw.execution.GaswOutputParser;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KJob;

public class KOutputParser extends GaswOutputParser {
    
    private File        stdOut;
    private File        stdErr;
    final private KJob  job;

    public KOutputParser(KJob job) {
        super(job.getData().getJobID());
        this.job = job;
    }

    @Override
    public GaswOutput getGaswOutput() throws GaswException {
        GaswExitCode gaswExitCode = GaswExitCode.EXECUTION_CANCELED;
        int exitCode;

        stdOut = getAppStdFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT);
        stdErr = getAppStdFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT);

        moveProvenanceFile(".");

        exitCode = parseStdOut(stdOut);
        exitCode = parseStdErr(stdErr, exitCode);

        switch (exitCode) {
            case 0:
                gaswExitCode = GaswExitCode.SUCCESS;
                break;
            case 1:
                gaswExitCode = GaswExitCode.ERROR_READ_GRID;
                break;
            case 2:
                gaswExitCode = GaswExitCode.ERROR_WRITE_GRID;
                break;
            case 6:
                gaswExitCode = GaswExitCode.EXECUTION_FAILED;
                break;
            case 7:
                gaswExitCode = GaswExitCode.ERROR_WRITE_LOCAL;
                break;
        }

        return new GaswOutput(job.getData().getJobID(), gaswExitCode, "", uploadedResults,
                appStdOut, appStdErr, stdOut, stdErr);
    }

    @Override
    protected void resubmit() throws GaswException {
        throw new GaswException("");
    }
}
