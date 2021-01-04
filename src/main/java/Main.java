import Step1.StepOne;
import Step2.StepTwo;
import Step3.StepThree;
import Step4.StepFour;
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
        boolean localAggregation = args.length > 0 && args[0].equals("-la");
        final String bucket = "s3://dsp211emr/";
        BasicConfigurator.configure();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Region.US_EAST_1.toString())
                .withCredentials(credentials)
                .build();
        HadoopJarStepConfig hadoopJarStepOne = new HadoopJarStepConfig()
                .withJar(bucket + "StepOne.jar")
                .withMainClass("StepOne")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", bucket + "output_step1/");

        StepConfig stepOneConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(hadoopJarStepOne)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStepTwo = new HadoopJarStepConfig()
                .withJar(bucket + "StepTwo.jar")
                .withMainClass("StepTwo")
                .withArgs(bucket + "output_step1/", bucket + "output_step2/");

        StepConfig stepTwoConfig = new StepConfig()
                .withName("StepTwo")
                .withHadoopJarStep(hadoopJarStepTwo)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStepThree = new HadoopJarStepConfig()
                .withJar(bucket + "StepThree.jar")
                .withMainClass("StepThree")
                .withArgs(bucket + "output_step1/",bucket + "output_step2/", bucket + "output_step3/");

        StepConfig stepThreeConfig = new StepConfig()
                .withName("StepThree")
                .withHadoopJarStep(hadoopJarStepThree)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStepFour = new HadoopJarStepConfig()
                .withJar(bucket + "StepFour.jar")
                .withMainClass("StepFour")
                .withArgs(bucket + "output_step3/", bucket + "output_step4/");

        StepConfig stepFourConfig = new StepConfig()
                .withName("StepFour")
                .withHadoopJarStep(hadoopJarStepFour)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStepFive = new HadoopJarStepConfig()
                .withJar(bucket + "StepFive.jar")
                .withMainClass("StepFive")
                .withArgs(bucket + "output_step4/", bucket + "output_step5/");

        StepConfig stepFiveConfig = new StepConfig()
                .withName("StepFive")
                .withHadoopJarStep(hadoopJarStepFive)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(10)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("dsp_key")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Assignment2_DSP")
                .withInstances(instances)
                .withSteps(stepThreeConfig, stepFourConfig, stepFiveConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withLogUri(bucket + "logs");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

