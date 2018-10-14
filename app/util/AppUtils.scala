package util

import java.security.MessageDigest

import akka.actor.FSM.->

/**
  * Created by ramesh on 9/13/17.
  */
class AppUtils {

  def getFolderName(user_id: String): String = {
    def encrypt_with_md5(text: String): String = {
      val encrypted_string = MessageDigest.getInstance("MD5").digest(text.getBytes)
        .map(0xFF & _).map {
        "%02x".format(_)
      }.foldLeft("") {
        _ + _
      }
      encrypted_string
    }

    val encrypted_value = encrypt_with_md5(user_id)
    encrypted_value.toString
  }

//  Number     |
//    | Percentage |
//  | Text       |
//  | Category   |
//  | Date

 def AthenaToAmplifyrDataTypeMap: Map[String, String] =
    Map(
      "DOUBLE" -> "Number",
      "DOUBLE" -> "Percentage",
      "VARCHAR" -> "Text",
      "VARCHAR" -> "Category",
      "VARCHAR" -> "Date" //Parquet in Athena does not support dates
    )

def AmplifyrToAthenaDataTypeMap: Map[String, String] =
    Map(
      "Number" -> "DOUBLE",
      "Percentage" -> "DOUBLE",
      "Text" -> "VARCHAR",
      "Category" -> "VARCHAR",
      "Date" -> "VARCHAR" //Parquet in Athena does not support dates
    )

}
