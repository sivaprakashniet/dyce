import java.sql.{Connection, DriverManager}

import com.amazonaws.services.s3.model.S3ObjectSummary
import filesystem.S3Config
import org.junit.runner._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test._

import scala.sys.process.Process

@RunWith(classOf[JUnitRunner])
class FileHandlerSecuredSpec extends Specification with Inject {

  def fixture = {
    var result: String = null
    val driver = configuration.underlying.getString("slick.dbs.default.db.driver")
    val url = configuration.underlying.getString("slick.dbs.default.db.url")
    val username = configuration.underlying.getString("slick.dbs.default.db.user")
    val password = configuration.underlying.getString("slick.dbs.default.db.password")
    val dbname = configuration.underlying.getString("dbname")

    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val select_statement = connection.createStatement()
      val trucate_tables = connection.createStatement()
      val rs = select_statement.executeQuery("SELECT TABLE_NAME as result FROM INFORMATION_SCHEMA.TABLES" +
        " where table_schema in ('"+dbname+"')")
      while (rs.next()) {
        var sql_query: String = "truncate table " + rs.getString("result")
        trucate_tables.execute(sql_query)
      }

      select_statement.executeUpdate("INSERT INTO  user (id, username, first_name, last_name, email, created_date, modified_date, profile_image)" +
        " VALUES('1','idealabs','idealabs','latentview','"+configuration.underlying.getString("authconfig.email")+"',23452345, 21341234,'');")

    } catch {
      case e: Exception => None
    }
    connection.close()
  }

  lazy val configuration = inject[play.api.Configuration]
  lazy val s3config = inject[S3Config]
  val s3client = s3config.s3client

  val local_filepath = TestDetails.local_filepath
  val s3path = TestDetails.s3path
  val user_id = TestDetails.user_id
  val incorrect_user_id = TestDetails.incorrect_user_id
  var dataset_id = ""
  var dataset_id2 = ""
  var column_id = ""
  val dataset_schema_before_schema_creation = TestDetails.dataset_schema_before_schema_creation
  val dataset_schema_after_schema_creation = TestDetails.dataset_schema_after_schema_creation
  val column_schema_before_schema_creation = TestDetails.column_schema_before_schema_creation
  val column_schema_after_schema_creation = TestDetails.column_schema_after_schema_creation
  val dataset_schema_rename = TestDetails.dataset_schema_rename
  val inferschema_test_json = TestDetails.inferschema_test_json
  val update_dataset_json = TestDetails.update_dataset_json

  val test_bucket = configuration.underlying.getString("s3config.bucket_name")
  val test_token = configuration.underlying.getString("authconfig.test_token")


  "FileHandlerSecured" should {

    val f = fixture

    "return a 404 on a bad request" in new WithApplication{
      val response = route(app, FakeRequest(POST, "/datasets/upload/presgnedurl-test")
        .withHeaders("Authorization"-> test_token))
      status(response.get) must equalTo(404)
    }

    "return a 404 on a request containing no file name" in new WithApplication{
      val response = route(app, FakeRequest(POST, "/datasets/upload/presignedurl")
        .withJsonBody(Json.parse("""{"file_path":""""+s3path+""""} """))
        .withHeaders("Authorization"-> test_token))
      status(response.get) must equalTo(404)
    }

    "returns a 401 on a request with an incorrect token" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/upload/presignedurl"))
        .withJsonBody(Json.parse("""{"file_path":""""+s3path+""""} """))
        .withHeaders("Authorization"-> "invalid")).get
      status(response) must equalTo(401)
    }

    "returns a 403 on a request with an incorrect user id" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + incorrect_user_id + "/datasets/upload/presignedurl"))
        .withJsonBody(Json.parse("""{"file_path":""""+s3path+""""} """))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(403)
    }

    "generates metadata for a dataset sample" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/inferschema"))
        .withJsonBody(Json.parse(inferschema_test_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "clear the test bucket" in new WithApplication {
      val command = Process("aws s3 rm s3://" + test_bucket + " --recursive")
      val execute_command = command.!
      (execute_command) mustEqual(0)
    }

    "check if there is no file in the test bucket" in new WithApplication {
      val objects_list = s3client.listObjects(test_bucket).getObjectSummaries.toArray.toList
      objects_list mustEqual(List())
    }

    "upload a file in the test bucket" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/upload/presignedurl" ))
        .withJsonBody(Json.parse("""{"file_path":""""+s3path+""""} """))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
      contentType(response) must beSome.which(_ == "application/json")
      val presignedurl_list = contentAsJson(response) \\ "presignedurl"

      presignedurl_list mustNotEqual(List())

      val command = Process("curl -v -X PUT -T " + local_filepath + " "
        + presignedurl_list(0).as[String])
      val execute_command = command.!
      (execute_command) mustEqual(0)

    }

    "check if there is a file in the test bucket with the same name" in new WithApplication {
      val objects_list = s3client.listObjects(test_bucket).getObjectSummaries.toArray.toList
      objects_list mustNotEqual(List())

      if(!(objects_list.isEmpty)) {
        val key = (objects_list(0).asInstanceOf[S3ObjectSummary].getKey.split("/"))(1)
        "/"+key mustEqual(s3path)
      }
    }

    "save file metadata in database" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/metadata"))
        .withJsonBody(Json.parse(dataset_schema_before_schema_creation))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      val dataset_id_opt = response_body \\ "dataset_id"
      dataset_id_opt mustNotEqual(List())
      if(!dataset_id_opt.isEmpty) {
        dataset_id = dataset_id_opt(0).as[String]
      }
    }

    "save file metadata2 in database" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/metadata"))
        .withJsonBody(Json.parse(dataset_schema_before_schema_creation))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      val dataset_id_opt = response_body \\ "dataset_id"
      dataset_id_opt mustNotEqual(List())
      if(!dataset_id_opt.isEmpty) {
        dataset_id2 = dataset_id_opt(0).as[String]
      }
    }

    "update file metadata in database" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + user_id + "/datasets/" + dataset_id + "/metadata"))
        .withJsonBody(Json.parse(dataset_schema_after_schema_creation))
        .withHeaders("Authorization"-> test_token)).get

      status(response) must equalTo(OK)
    }

    "save column metadata in database" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/column-metadata"))
        .withJsonBody(Json.parse(column_schema_before_schema_creation))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "update column metadata in database" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + user_id + "/datasets/" + dataset_id + "/column-metadata"))
        .withJsonBody(Json.parse(column_schema_after_schema_creation))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "get column metadata from the database" in new WithApplication {
      val response = route(app, FakeRequest(GET, ("/" + user_id + "/datasets/" + dataset_id + "/column-metadata"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
      val json_response = contentAsJson(response)
      column_id = (json_response \\ "id")(0).toString()
    }

    "create univariate summary for the dataset" in new WithApplication {

      val univariate_summary_json = """{
                                       "dataset_id": """"+dataset_id+"""",
                                       "dataset_name": "sample",
                                       "dataset_path": "/datasets/sample.csv",
                                       "columns": [
                                         {
                                           "column_id": """"+column_id+"""",
                                           "column_name": "Segment",
                                           "position":0,
                                           "column_datatype": "String",
                                           "number_of_bins": 12,
                                           "decimal": 0
                                         }
                                       ]}"""

      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/univariate"))
        .withJsonBody(Json.parse(univariate_summary_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "create default univariate summary for a single column in the dataset" in new WithApplication {

      val custom_univariate_summary_json = """[
                                        {
                                          "column_id": "6dc7a935-6576-4e2c-b182-ca2400362aaf",
                                          "column_name": "Length",
                                          "position": 2,
                                          "decimal": 2,
                                          "column_datatype": "Number",
                                          "number_of_bins": 14,
                                          "bin_ends": [
                                            0.05,
                                            0.95
                                          ]
                                        }
                                      ]"""

      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/numeric-binning"))
        .withJsonBody(Json.parse(custom_univariate_summary_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "create custom univariate summary for a single column in the dataset" in new WithApplication {

      val custom_univariate_summary_json = """[{ "column_id": "d6664d8a-fd5a-4334-bc64-0b616a9de2e9",
                                                 "column_name": "Length",
                                                 "position": 2,
                                                 "decimal": 2,
                                                 "column_datatype": "Number",
                                                 "number_of_bins": 12,
                                                 "bins": ["0.07","0.28","0.32",
                                                   "0.36","0.41","0.45",
                                                   "0.48","0.53","0.56",
                                                   "0.60","0.65","0.69","0.81"]}]"""

      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/numeric-binning"))
        .withJsonBody(Json.parse(custom_univariate_summary_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "create default univariate summary for multiple columns the dataset" in new WithApplication {

      val custom_univariate_summary_json = """[
                                               {
                                                 "column_id": "6dc7a935-6576-4e2c-b182-ca2400362aaf",
                                                 "column_name": "Sex",
                                                 "position": 1,
                                                 "decimal": 0,
                                                 "column_datatype": "Category",
                                                 "number_of_bins": 22,
                                                 "bin_ends": [
                                                   0.05,
                                                   0.95
                                                 ]
                                               },
                                               {
                                                 "column_id": "3fc1f333-df6d-4afa-b662-349878570876",
                                                 "column_name": "Length",
                                                 "position": 2,
                                                 "decimal": 2,
                                                 "column_datatype": "Number",
                                                 "number_of_bins": 22,
                                                 "bin_ends": [
                                                   0.05,
                                                   0.95
                                                 ]
                                               }
                                             ]"""

      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/numeric-binning"))
        .withJsonBody(Json.parse(custom_univariate_summary_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    // Manage dataset testcases

    //get datasets
    "list datasets in  database" in new WithApplication {
      val response = route(app, FakeRequest(GET, ("/" + user_id + "/datasets/" + dataset_id + "/metadata"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    //dataset rename
    "returns 200 while performing update operation to the valid dataset" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + user_id + "/datasets/" + dataset_id + "/metadata"))
        .withJsonBody(Json.parse(dataset_schema_rename))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    //dataset rename
    "returns 403 while performing update operation to the incorrect dataset" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + incorrect_user_id + "/datasets/" + dataset_id + "/metadata"))
        .withJsonBody(Json.parse(dataset_schema_rename))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(403)
    }

    "returns 409 while performing update operation to the dataset with duplidate dataset name" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + user_id + "/datasets/" + dataset_id2 + "/metadata"))
        .withJsonBody(Json.parse(dataset_schema_rename))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(409)
    }

    "returns 403 while performing delete operation to the incorrect dataset" in new WithApplication {
      val response = route(app, FakeRequest(DELETE, ("/" + incorrect_user_id + "/datasets/" + dataset_id + "/metadata"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(403)
    }

    //
    //get datasets
    "getting univariate summary for a dataset_id" in new WithApplication {
      val response = route(app, FakeRequest(GET, ("/" + user_id + "/datasets/" + dataset_id + "/univariate"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

    "updating a dataset for valid dataset_id" in new WithApplication {
      val response = route(app, FakeRequest(PUT, ("/" + user_id + "/datasets/" + dataset_id + "/view"))
        .withJsonBody(Json.parse(update_dataset_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    "create decision tree for a dataset_id" in new WithApplication {
      val create_d_tree_json =
        """{
      "name": "Testing decision tree",
      "input_column": ["Segment"],
      "output_column": "Marks_in_maths",
      "dataset_id": """"+dataset_id+"""",
      "dataset_name": "sample.csv",
      "parameters":
      {
      "impurity": "entropy",
      "max_bins": 32,
      "max_depth": 5,
      "min_instances_per_node": 1,
      "training_data": 80,
      "test_data": 20,
      "nfolds":0
      }
      }""".stripMargin
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/decision-tree"))
        .withJsonBody(Json.parse(create_d_tree_json))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    "getting decision trees for a dataset_id" in new WithApplication {
      val response = route(app, FakeRequest(GET, ("/" + user_id + "/datasets/" + dataset_id + "/decision-tree"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    "create random forests for a dataset" in new WithApplication {

      val create_random_forest = """{
                                     "name": "RF-1",
                                     "dataset_id": """"+dataset_id+"""",
                                     "dataset_name": "sample_dataset",
                                     "input_column": [
                                       "MOB",
                                       "AMF"
                                     ],
                                     "output_column": "Segment",
                                     "parameters": {
                                       "impurity": "gini",
                                       "max_bins": 32,
                                       "max_depth": 5,
                                       "min_instances_per_node": 1,
                                       "training_data": 80,
                                       "test_data": 20,
                                       "max_classes": 2,
                                       "feature_subset_strategy": "Auto",
                                       "num_trees": 100,
                                       "nfolds":0
                                     }
                                   }"""
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/random-forest"))
        .withJsonBody(Json.parse(create_random_forest))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    "create gradient boosted trees for a dataset" in new WithApplication {
      val create_gbt = """{
                           "name": "GBT-1",
                           "dataset_id": """"+dataset_id+"""",
                           "dataset_name": "sample_dataset",
                           "input_column": [
                             "MOB",
                             "AMF"
                           ],
                           "output_column": "Segment",
                           "parameters": {
                             "max_bins": 32,
                             "max_depth": 5,
                             "min_instances_per_node": 1,
                             "training_data": 80,
                             "test_data": 20,
                             "num_iterations": 10,
                             "nfolds":0
                           }
                         }"""
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/gradient-boosted-tree"))
        .withJsonBody(Json.parse(create_gbt))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    "find anomalies in a dataset" in new WithApplication {
      val find_anomalies = """{
                               "name": "AM",
                               "trees": 20,
                               "sample_proportions": 50,
                               "no_of_anomalies": 100,
                               "features": 2,
                               "dataset_id": """"+dataset_id+"""",
                               "dataset_name": "sample_dataset",
                               "bootstrap": false,
                               "replacement": true,
                               "feature_columns": [
                                 "Segment",
                                 "MOB"
                               ],
                               "feature_column_ids": [
                                 "73896076-2a61-410b-804a-e7ab07658056",
                                 "285c4aba-5349-46f0-b9f7-5c827d2c7dcb"
                               ],
                               "random_split": 65
                             }"""
      val response = route(app, FakeRequest(POST, ("/" + user_id + "/datasets/" + dataset_id + "/anomaly"))
        .withJsonBody(Json.parse(find_anomalies))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(200)
    }

    //dataset delete
    "returns 200 while performing delete operation to the valid dataset" in new WithApplication {
      val response = route(app, FakeRequest(DELETE, ("/" + user_id + "/datasets/" + dataset_id + "/metadata"))
        .withHeaders("Authorization"-> test_token)).get
      status(response) must equalTo(OK)
    }

  }
}