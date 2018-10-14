import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client

object S3Config {

  val access_key = "AKIAIHBBE5LFMW3KEGGQ"
  val secret_key = "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"
  val region = "us-east-1"

  val bucket_name = "amplifyr-users"
  val bucket_name_for_results = "amplifyr-v2-job-data"
  val expiration_time = 3600000

  val awsCreds = new BasicAWSCredentials(access_key, secret_key)

  val s3client = new AmazonS3Client(new AWSStaticCredentialsProvider(awsCreds))
  s3client.setRegion(Region.getRegion(Regions.US_EAST_1))

}


