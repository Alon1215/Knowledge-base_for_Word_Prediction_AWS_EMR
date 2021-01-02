import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class Main {

    public static void main(String[] args) {
        final String bucket = "s3://dsp211emr/";
        BasicConfigurator.configure();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Region.US_EAST_1.toString())
                .withCredentials(credentials)
                .build();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("target/stepone.jar")
                .withMainClass("StepOne")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", bucket + "output_step1");

        StepConfig stepConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

//        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
//                .withInstanceCount(2)
//                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
//                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
//                .withHadoopVersion("2.6.0").withEc2KeyName("dsp_key")
//                .withKeepJobFlowAliveWhenNoSteps(false)
//                .withPlacement(new PlacementType("us-east-1a"));

//        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//                .withName("Assignment2_DSP")
//                .withInstances(instances)
//                .withSteps(stepConfig)
//                .withServiceRole("EMR_DefaultRole")
//                .withJobFlowRole("EMR_EC2_DefaultRole")
//                .withReleaseLabel("emr-5.11.0")
//                .withLogUri( bucket + "logs");
//
//        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
//        String jobFlowId = runJobFlowResult.getJobFlowId();
//        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

