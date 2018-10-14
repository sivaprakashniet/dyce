import java.sql.{Connection, DriverManager}

import org.junit.runner._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._

@RunWith(classOf[JUnitRunner])
class AccessControlListSpec extends Specification with Inject {
  lazy val configuration = inject[play.api.Configuration]
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
      val create_perm_statement = connection.createStatement()
      val trucate_tables = connection.createStatement()
      val rs = select_statement.executeQuery("SELECT TABLE_NAME as result FROM INFORMATION_SCHEMA.TABLES" +
        " where table_schema in ('"+dbname+"')")
      while (rs.next()) {
        var sql_query: String = "truncate table " + rs.getString("result")
        trucate_tables.execute(sql_query)
      }
      select_statement.executeUpdate("INSERT INTO  user (id, username, first_name, last_name, email, created_date, modified_date, profile_image)" +
        " VALUES('1','idealabs','idealabs','latentview','"+configuration.underlying.getString("authconfig.email")+"',2345234523,234523452,'');")
      create_perm_statement.executeUpdate("INSERT INTO acl (id, user_id, object_id, resource_type, privilege, permission) VALUES ('1', '1', '1', 'DatasetAPI', '*', 'allow'),('2', '1', '2', 'DatasetAPI', 'read', 'allow');")


    } catch {
      case e: Exception => None
    }
    connection.close()
  }


  "AccessControlList controller" should {
    val f = fixture
    var token = ""
    var cookie: play.api.mvc.Cookie = null
    val sample_token_user = configuration.underlying.getString("authconfig.test_token")

    // Owner of the dataset can to all the actions like read, update, delete
    "Returns 200 while performing read operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/datasets/1/resource/1")
                      .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    "Returns 200 while performing update operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(PUT, "/datasets/1/resource/1")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    "Returns 200 while performing delete operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(DELETE, "/datasets/1/resource/1")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    "Returns 403 while performing read operation on the incorrect resource" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/datasets/2/resource/1")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(403)
    }

    "Returns 403 while performing update operation on the incorrect resource" in new WithApplication {
      val result = route(app, FakeRequest(PUT, "/datasets/2/resource/1")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(403)
    }

    "Returns 403 while performing delete operation on the incorrect resource" in new WithApplication {
      val result = route(app, FakeRequest(DELETE, "/datasets/2/resource/1")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(403)
    }
    // Testcase for read permission
    "Returns 200 for valid user when getting access to protected resource with accesstoken  " in new WithApplication {
      val result = route(app, FakeRequest(GET, "/datasets/1/resource/2")
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    //A 404 (Not Found) status code should be returned if the url is incorrect
    "sends a 404 on a bad URL" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/boum")).get
      status(result) must equalTo(404)
    }
  }
}
