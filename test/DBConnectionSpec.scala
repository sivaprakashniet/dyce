import java.sql.{Connection, DriverManager}

import org.junit.runner._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test._

@RunWith(classOf[JUnitRunner])
class DBConnectionSpec extends Specification with Inject {
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


  "DBConnection controller" should {
    val f = fixture
    var token = ""
    var cookie: play.api.mvc.Cookie = null
    val sample_token_user = configuration.underlying.getString("authconfig.test_token")
    var connection_id = ""

    // Owner of the dataset can to all the actions like read, update, delete
    "Returns 200 while performing create operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(POST, "/1/db-connection")
        .withJsonBody(Json.parse("""{"connection_name": "testing","connection_method": "Standard(TCP/IP)","host_name": "localhost","port":3306,"username": "root","password": "fubar","default_schema": "mysql","connection_db_type": "mysql"}"""))
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    "Returns 200 while performing get operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/1/db-connection")
        .withHeaders("Authorization"->sample_token_user)).get
      val json_response = contentAsJson(result)
      connection_id = ((json_response \\ "connections")(0) \\ "id").toList(0).as[String]
      status(result) must equalTo(200)
    }

    "Returns 200 while performing get One operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(GET, "/1/db-connection/"+connection_id)
        .withHeaders("Authorization"->sample_token_user)).get
      val json_response = contentAsJson(result)
      status(result) must equalTo(200)
    }

    "Returns 200 while performing create operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(PUT, "/1/db-connection/"+connection_id)
        .withJsonBody(Json.parse("""{"connection_name": "testing","connection_method": "Standard(TCP/IP)","host_name": "localhost","port":3306,"username": "root","password": "fubar","default_schema": "mysql","connection_db_type": "mysql"}"""))
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

  // Create test connections
    "Returns 200 while performing test connection with valid data" in new WithApplication {
      val result = route(app, FakeRequest(POST, "/1/test-db-connection")
        .withJsonBody(Json.parse("""{"connection_name": "testing","connection_method": "Standard(TCP/IP)","host_name": "localhost","port":3306,"username": "root","password": "fubar","default_schema": "mysql","connection_db_type": "mysql"}"""))
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(200)
    }

    "Returns 500 while performing test connection with invalid data" in new WithApplication {
      val result = route(app, FakeRequest(POST, "/1/test-db-connection")
        .withJsonBody(Json.parse("""{"connection_name": "testing","connection_method": "Standard(TCP/IP)","host_name": "localhost","port":3306,"username": "43523542345","password": "fubar","default_schema": "mysql","connection_db_type": "mysql"}"""))
        .withHeaders("Authorization"->sample_token_user)).get
      status(result) must equalTo(500)
    }

    "Returns 200 while performing delete operation on the correct resource" in new WithApplication {
      val result = route(app, FakeRequest(DELETE, "/1/db-connection/"+connection_id)
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
