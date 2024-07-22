package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.io.File;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswExitCode;
import fr.insalyon.creatis.gasw.GaswOutput;
import fr.insalyon.creatis.gasw.execution.GaswOutputParser;

public class K8sOutputParser extends GaswOutputParser {

    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private File stdOut;
    private File stdErr;

    public K8sOutputParser(String jobID) {
        super(jobID);
    }

    @Override
    public GaswOutput getGaswOutput() throws GaswException {

        stdOut = getAppStdFile(GaswConstants.OUT_EXT, GaswConstants.OUT_ROOT);
        stdErr = getAppStdFile(GaswConstants.ERR_EXT, GaswConstants.ERR_ROOT);

        moveProvenanceFile(".");

        int exitCode = parseStdOut(stdOut);
        exitCode = parseStdErr(stdErr, exitCode);

        GaswExitCode gaswExitCode = GaswExitCode.UNDEFINED;
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

        return new GaswOutput(job.getId(), gaswExitCode, "", uploadedResults,
                appStdOut, appStdErr, stdOut, stdErr);
    }

    @Override
    protected void resubmit() throws GaswException {
        throw new GaswException("");
    }
}
