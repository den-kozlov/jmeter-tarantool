package com.bercut.sampler;

import com.bercut.handlers.ITarantoolHandler;
import com.bercut.handlers.TarantoolHandlerFactory;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.TarantoolClient;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

import static com.bercut.constants.TarantoolArguments.*;

public class TarantoolSampler extends AbstractJavaSamplerClient implements Serializable {

    public static final String ENCODING = "UTF-8";
    private static final long serialVersionUID = 3L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TarantoolSampler.class);;

    private ITarantoolHandler tarHandler;

    // set up default arguments for the JMeter GUI
    @Override
    public org.apache.jmeter.config.Arguments getDefaultParameters() {
        org.apache.jmeter.config.Arguments defaultParameters = new org.apache.jmeter.config.Arguments();
        defaultParameters.addArgument(METHOD, "GET");
        defaultParameters.addArgument(SERVERS, "127.0.0.1");
        defaultParameters.addArgument(USERNAME, "");
        defaultParameters.addArgument(PASSWORD, "");
        defaultParameters.addArgument(SPACE_NAME, "");
        defaultParameters.addArgument(CLUSTER_DISCOVERY_FUNCTION, "");
        defaultParameters.addArgument(TIMEOUT, "10000");
        defaultParameters.addArgument(KEY, "");
        defaultParameters.addArgument(VALUE, "");
        defaultParameters.addArgument(LOCAL_FILE_PATH, "");
        defaultParameters.addArgument(DEBUG, "true");
        defaultParameters.addArgument(RANDOM_KEY, "false");
        defaultParameters.addArgument(RANDOM_KEY_MIN, "0");
        defaultParameters.addArgument(RANDOM_KEY_MAX, "999999");

        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        try {
            this.tarHandler = TarantoolHandlerFactory.getHandler(context);
        } catch (Exception ex) {
            LOGGER.error("setupTest: ", ex);
        }
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (this.tarHandler != null) {
            TarantoolClient client = tarHandler.getClient();

            if (client != null && client.isAlive()) {
                client.close();
            }
            this.tarHandler = null;
        }
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult result = newSampleResult();
        result.sampleStart(); // start stopwatch

        try {
            if (tarHandler == null || tarHandler.getClient() == null) {
                throw new Exception("Tarantool Client is not connected");
            }

            long startTime = System.nanoTime();

            tarHandler.handle();

            long endTime = System.nanoTime();

            sampleResultSuccess(result, Long.toString(endTime - startTime));

        } catch (Exception e) {
            sampleResultFailed(result, e);

            // get stack trace as a String to return as document data
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            result.setResponseData(stringWriter.toString(), ENCODING);
            result.setDataType(SampleResult.TEXT);
            result.setResponseCode("500");

            LOGGER.error("runTest: ", e);
        }

        return result;
    }

    public SampleResult newSampleResult() {
        SampleResult result = new SampleResult();
        result.setDataEncoding(ENCODING);
        result.setDataType(SampleResult.TEXT);
        return result;
    }

    /**
     * Set the sample result as <code>sampleEnd()</code>,
     * <code>setSuccessful(true)</code>, <code>setResponseCodeOK()</code> and if
     * the response is not <code>null</code> then
     * <code>setResponseData(response.toString(), ENCODING)</code> otherwise it is
     * marked as not requiring a response.
     *
     * @param result   sample result to change
     * @param response the successful result message, may be null.
     */
    protected void sampleResultSuccess(SampleResult result,
                                       String response) {
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        if (response != null) {
            result.setResponseData(response, ENCODING);
            result.setResponseMessage(response);
        } else {
            result.setResponseData("No response required", ENCODING);
        }
    }

    /**
     * Mark the sample result as <code>sampleEnd</code>,
     * <code>setSuccessful(false)</code> and the <code>setResponseCode</code> to
     * reason.
     *
     * @param result the sample result to change
     * @param reason the failure reason
     */
    protected void sampleResultFailed(SampleResult result, String reason) {
        result.sampleEnd();
        result.setSuccessful(false);
        result.setResponseCode(reason);
        result.setResponseData(reason, ENCODING);
        result.setResponseMessage(reason);
    }

    /**
     * Equivalent to
     * <code>sampleResultFailed(result, "Exception raised: " + cause)</code>
     *
     * @param result the result to modify
     * @param cause  the cause of the failure
     */
    protected void sampleResultFailed(SampleResult result, Exception cause) {
        sampleResultFailed(result, "Exception raised: " + cause);
    }
}

