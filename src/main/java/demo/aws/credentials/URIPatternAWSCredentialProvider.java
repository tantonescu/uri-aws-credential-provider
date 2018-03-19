package demo.aws.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * AWS credential provider that allows cluster to assume a role when accessing S3 URIs that match predefined pattern.
 * <p>
 * The credential provider should receive as part of Hadoop config:
 * - AWS role ARN {@link URIPatternAWSCredentialProvider#AWSCP_ROLE_ARN_KEY}
 * - URI pattern {@link URIPatternAWSCredentialProvider#AWSCP_URI_PATTERN_KEY}
 * If the configs are not present then it will fallback to default AWS credentials provider
 * </p>
 */

public class URIPatternAWSCredentialProvider implements AWSCredentialsProvider, Configurable {

    /**
     * Config key for role ARN to assume
     */
    public final static String AWSCP_ROLE_ARN_KEY = "awscp-role-arn";

    /**
     * Config key for URI pattern requiring role assume
     */
    public final static String AWSCP_URI_PATTERN_KEY = "awscp-uri-pattern";

    /**
     * Time before expiry within which credentials will be renewed.
     */
    private static final int EXPIRE_MILLISECONDS = 60 * 1000;

    /**
     * Life span of the temporary credentials requested from STS
     */
    private static final int TEMP_CRED_DUR_SECONDS = 3600;

    /**
     * Accessed Uri
     */
    private final URI uri;

    /**
     * Hadoop configuration
     */
    private Configuration configuration;

    /**
     * From Config: URI pattern requiring role assume
     */
    private String uriPattern;

    /**
     * From Config: ARN of the role to be assumed
     */
    private String roleArn;

    /**
     * Returned credentials
     */
    private AWSCredentials credentials;

    /**
     * STS credentials
     */
    private static Credentials stsCredentials;

    /**
     * Instance Profile credentials
     */
    private static InstanceProfileCredentialsProvider instanceCredentials;


    private Logger logger = LogManager.getLogger(URIPatternAWSCredentialProvider.class);

    /**
     * Create a {@link AWSCredentialsProvider} from an URI and configuration params.
     * The role is assumed to provide credentials for downstream operations.
     * <p>
     * The constructor signature must conform to hadoop calling convention exactly.
     * </p>
     *
     * @param uri  An URI
     * @param conf Hadoop config
     */
    public URIPatternAWSCredentialProvider(URI uri, Configuration conf) {
        this.uri = uri;
        this.configuration = conf;
        this.roleArn = conf.get(AWSCP_ROLE_ARN_KEY);
        this.uriPattern = conf.get(AWSCP_URI_PATTERN_KEY);

        if (this.roleArn == null || this.uriPattern == null) {
            logger.info("URIPatternAWSCredentialProvider. Role / uri pattern not provided via Hadoop configuration.");
        } else {
            logger.info("URIPatternAWSCredentialProvider. Role: " + this.roleArn + ". URI pattern: " + this.uriPattern);
        }
    }

    @Override
    public AWSCredentials getCredentials() {
        this.logger.info("URIPatternAWSCredentialProvider. getCredentials() called.");

        if (this.roleArn == null || this.uriPattern == null) {
            this.logger.info("URIPatternAWSCredentialProvider. Assume role / uri pattern not provided.");
        } else if (uri.toString().matches(this.uriPattern)) {
            if (stsCredentials == null ||
                    (stsCredentials.getExpiration().getTime() - System.currentTimeMillis() < EXPIRE_MILLISECONDS)) {

                String sessionName = "awscp-" + java.util.UUID.randomUUID().toString();
                this.logger.info("URIPatternAWSCredentialProvider. Assuming role: " + this.roleArn +
                        ". Session: " + sessionName);

                AWSSecurityTokenServiceClient stsClient =
                        new AWSSecurityTokenServiceClient(new ProfileCredentialsProvider());

                //Assuming the role in the other account to obtain temporary credentials
                AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                        .withRoleArn(this.roleArn)
                        .withDurationSeconds(TEMP_CRED_DUR_SECONDS)
                        .withRoleSessionName(sessionName);

                AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);

                stsCredentials = assumeResult.getCredentials();
            }

            this.credentials = new BasicSessionCredentials(
                    stsCredentials.getAccessKeyId(),
                    stsCredentials.getSecretAccessKey(),
                    stsCredentials.getSessionToken());
        } else {
            this.logger.info("URIPatternAWSCredentialProvider. Assume not needed. Using InstanceProfile role.");

            if (instanceCredentials == null) {
                instanceCredentials = new InstanceProfileCredentialsProvider(true);
            }
            this.credentials = instanceCredentials.getCredentials();
        }
        return this.credentials;
    }

    @Override
    public void refresh() {
        logger.info("URIPatternAWSCredentialProvider. refresh() called.");
    }

    @Override
    public void setConf(Configuration configuration) {
    }

    @Override
    public Configuration getConf() {
        logger.info("URIPatternAWSCredentialProvider. getConf() called.");
        return this.configuration;
    }

}
