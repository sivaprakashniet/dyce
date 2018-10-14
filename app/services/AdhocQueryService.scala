package services

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}
import java.util.Properties
import javax.inject._

import play.api.Configuration
import dao._
import entities._
import filesystem.FilesystemService
import util.AppUtils
import views.html.user

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
  * This class implements Adhoc querying based on Amazon Athena. The key
  * methods are: executeAdhocQuery and initAdhocQueryCtx
  *
  * The web service first calls initAdhocQueryCtx, which gets the metadata to
  * the end user. All the subsequent queries are based on this metadata, which
  * can be called through executeAdhocQuery method
  *
  * @param userService
  * @param filesystemService
  *
  */

class AdhocQueryService @Inject()(userService: UserService,
                                  filesystemService: FilesystemService,
                                  utils: AppUtils,
                                  config: Configuration
                                 ) {


  /**
    * Executes the adhoc query and returns the result as a [[DataView]]
    * @param adhoc_query_params
    * @return
    */
  def executeAdhocQuery(adhoc_query_params: AdhocQueryParam,
                        user_id: String): Future[List[Map[String, String]]]
  = Future {

    val query = adhoc_query_params.sql_query

    executeQuery(query, user_id)
  }


  /**
    * This method initializes the query context before any ad-hoc query can be
    * executed.
    *
    * This will clear existing extern tables in Athena and define a new one
    * based on the most recently available metadata
    *
    */
  def initAdhocQueryCtx(dataset_metadata: FileMetadata,
                        column_medata: List[ColumnMetadata], user_id: String):
  Boolean
  = {
    dropExternTable(dataset_metadata.parquet_file_path.get.replace
    ("/parquet_datasets/",""), user_id)
    createExternTable(dataset_metadata, column_medata, user_id)
  }


  private def dropExternTable(dataset_name: String, user_id: String): Boolean
  = {

    val q = s"DROP TABLE IF EXISTS $dataset_name"

    execute(q, user_id)

  }


  private def createExternTable(dataset_metadata: FileMetadata,
                                column_medata: List[ColumnMetadata], user_id:
                                String): Boolean
  = {

    val columnsString = column_medata map {
      col => col.name + " " +
        utils.AmplifyrToAthenaDataTypeMap.getOrElse(col.datatype, "VARCHAR")
    } mkString(",")

    val location = filesystemService.getS3ParquetLocation(dataset_metadata.user_id.get)

    val q =
      s"""
        CREATE EXTERNAL TABLE ${dataset_metadata.parquet_file_path.get.replace("/parquet_datasets/","")} (

          ${columnsString}

        ) STORED AS PARQUET
        LOCATION '$location'
      """

    execute(q, user_id)
  }

  def initProperties(user_id: String): Properties = {

    val info: Properties = new Properties()
    info.put("user", config.underlying.getString("adhoc.db.user"));
    info.put("password", config.underlying.getString("adhoc.db.password"));
    // info.put("s3_staging_dir", filesystemService.getS3ParquetLocation(user_id))
    info.put("s3_staging_dir", "s3://amplifyr-users/70efdf2ec9b086079795c442636b55fb/parquet_datasets/")
    info.put("aws_credentials_provider_class","com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    info
  }

  def execute(command: String, user_id: String): Boolean = {

    val props: Properties =  initProperties(user_id)
    var result = false

    println(props)

    Class.forName(config.underlying.getString("adhoc.db.driver"));

    var connection:Connection = null

    try {

      connection = DriverManager.getConnection(config.underlying.getString("adhoc.db" +
        ".url"), props);

      val statement = connection.createStatement();

      println(command)

      result = statement.execute(command)

    } catch {

      case e => e.printStackTrace()

    } finally {

      if(connection != null)
        connection.close()
    }

    result

  }

  def executeQuery(query: String, user_id: String): List[Map[String, String]] = {

    val props: Properties =  initProperties(user_id)
    var result = ListBuffer[Map[String, String]]()

    Class.forName(config.underlying.getString("adhoc.db.driver"));

    var connection:Connection = null

    try {

      connection = DriverManager.getConnection(config.underlying.getString("adhoc.db" +
        ".url"), props);

      val statement = connection.createStatement();

      println(query)

      val rs: ResultSet = statement.executeQuery(query)

      val rsmd: ResultSetMetaData = rs.getMetaData()

      val noOfColumns = rsmd.getColumnCount()

      val columnLabels: Seq[String] = for(i <- 1 to noOfColumns)
        yield rsmd.getColumnLabel(i)

      while(rs.next()) {

        val row: Seq[String] = for(i <- 1 to noOfColumns) yield rs.getString(i)

        result += ((columnLabels zip row) toMap)
      }


    } catch {

      case e => e.printStackTrace()

    } finally {

      if(connection != null)
        connection.close()
    }

    result.toList

  }


}
