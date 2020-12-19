import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import org.apache.hadoop.security.HadoopKerberosName;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class Main {

    public static void main(String[] args) {
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

    }
}

