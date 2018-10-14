import java.sql.{Connection, DriverManager}

import org.junit.runner._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._

@RunWith(classOf[JUnitRunner])
class AuthenticationSpec extends Specification with Inject {
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
      val trucate_tables = connection.createStatement()
      val rs = select_statement.executeQuery("SELECT TABLE_NAME as result FROM INFORMATION_SCHEMA.TABLES" +
        " where table_schema in ('"+dbname+"')")
      while (rs.next()) {
        var sql_query: String = "truncate table " + rs.getString("result")
        trucate_tables.execute(sql_query)
      }
      select_statement.executeUpdate("INSERT INTO  user (id, username, first_name, last_name, email, created_date, modified_date, profile_image)" +
        " VALUES('1','idealabs','idealabs','latentview','"+configuration.underlying.getString("authconfig.email")+"',32452345, 23452345,'');")
    } catch {
      case e: Exception => None
    }
    connection.close()
  }


  "Authentication controller" should {
    val f = fixture
    var token = ""
    var cookie: play.api.mvc.Cookie = null
    val sample_token_user = configuration.underlying.getString("authconfig.test_token")
    val sample_token = "ya29.Glw_BE9VDHIaAZJh6mLWqBbWvvwbsCtVIzSpnfL6AIJPWf6HrKI0oCmdLs5PXP4zCIt-2qWSN0_8pj62NLmhN5IgRzun-1dKYQskFadGn_qs5GvJcLRq_3IEyWDMQA"

    "Returns 401 unauthorized when getting users without accesstoken  " in new WithApplication {
      val result = route(app, FakeRequest(GET, "/users")).get
      status(result) must equalTo(401)
    }

    "Returns 401 unauthorized when getting users with invalid accesstoken " in new WithApplication {
      val result = route(app, FakeRequest(GET, "/users").withHeaders("Authorization"->"invalidToken")).get
      status(result) must equalTo(401)
    }

    "Returns 403 forbidden when getting users with valid accesstoken and invalid user" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/users").withHeaders("Authorization"->sample_token)).get
      status(result) must equalTo(403)
    }

    "Returns 200 when getting users with valid access token and user" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/users").withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    //A 404 (Not Found) status code should be returned if the url is incorrect
    "sends a 404 on a bad URL" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/boum")).get
      status(result) must equalTo(404)
    }
  }
}
