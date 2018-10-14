package util

class InferSchema {
  type DataPreview = List[List[String]]
  type PartialSchema = List[Map[String, Any]]

  def infer_schema(data_preview: DataPreview): PartialSchema = {
    val min_distinct_values_for_string = 20

    //Regular expressions
    //val dateRegex = """([0-9]{2}-[0-9]{2}-[0-9]{4}|[0-9]{2}-[0-9]{2}-[0-9]{2}
    // |[0-9]{2}/[0-9]{2}/[0-9]{4}|[0-9]{2}/[0-9]{2}/[0-9]{2})"""

    val dateRegex1 =
    """([0-9]{2}-[0-9]{2}-[0-9]{4})"""
    //DD-MM-YYYY
    val dateRegex2 =
      """([0-9]{4}-[0-9]{2}-[0-9]{2})"""
    //YYYY-MM-DD

    val dateRegex3 =
      """([0-9]{2}/[0-9]{2}/[0-9]{2})"""

    val dateRegex4 =
      """([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)"""
      //"""(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z"""
    //DD-MM-YY

    val dateRegex5 = """([0-9]{2}-[0-9]{2}-[0-9]{2})"""
    //DD/MM/YY

    val dateRegex6 =
      """([0-9]{2}/[0-9]{2}/[0-9]{4})"""
    //YYYY-MM-DD


    val timeRegex =
      """([0-9]{2}:[0-9]{2}:[0-9]{2})"""
    //HH:MM:SS
    val numRegex =
      """(\-?[0-9]+\.?[0-9]*e?[0-9]*)"""

    //Conversion to regex format
    val NumberFmt = numRegex.r
    val DateFmt1 = dateRegex1.r
    val DateFmt2 = dateRegex2.r
    val DateFmt3 = dateRegex3.r
    val DateFmt5 = dateRegex5.r
    val DateFmt6 = dateRegex6.r

    val DateTimeFmt1 = (dateRegex1 + " " + timeRegex).r
    val DateTimeFmt2 = (dateRegex2 + " " + timeRegex).r
    val DateTimeFmt3 = (dateRegex3 + " " + timeRegex).r
    val DateTimeFmt4 = dateRegex4.r

    val TimeFmt = timeRegex.r

    def datatype_map_generator(d: String, f: String, dec: Int) =
      Map("datatype" -> d, "format" -> f, "decimal" -> dec)

    def calcDecimalPlaces(s: String): Int = {
      if (s.contains(".")) 2 else 0
    }

    def null_datatype_map = datatype_map_generator("Null","",0)
    def text_datatype_map = datatype_map_generator("Text","Text",0)
    def category_datatype_map = datatype_map_generator("Category","Category",0)
    def percentage_datatype_map(s: String) =
      datatype_map_generator("Percentage","",calcDecimalPlaces(s))
    def number_datatype_map(s: String) =
      datatype_map_generator("Number","",calcDecimalPlaces(s))


    def date1_datatype_map = datatype_map_generator("Date","DD-MM-YYYY",0)
    def date2_datatype_map = datatype_map_generator("Date","YYYY-MM-DD",0)
    def date3_datatype_map = datatype_map_generator("Date","DD/MM/YY",0)
    def date5_datatype_map = datatype_map_generator("Date","DD-MM-YY",0)
    def date6_datatype_map = datatype_map_generator("Date","DD/MM/YYYY",0)

    def datetime1_datatype_map = datatype_map_generator("Date","DD-MM-YYYY HH:MM:SS",0)
    def datetime2_datatype_map = datatype_map_generator("Date","YYYY-MM-DD HH:MM:SS",0)
    def datetime3_datatype_map = datatype_map_generator("Date","DD/MM/YY HH:MM:SS",0)
    // ISO WithZone
    def datetime4_datatype_map = datatype_map_generator("Date","YYYY-MM-DDThh:mm:ssZ",0)


    def getSingleValueDatatype(s: String): Map[String, Any] = s.trim match {
      case DateFmt1(d) => date1_datatype_map
      case DateFmt2(d) => date2_datatype_map
      case DateFmt3(d) => date3_datatype_map
      case DateFmt5(d) => date5_datatype_map
      case DateFmt6(d) => date6_datatype_map
      // WithZone
      case DateTimeFmt4(d) =>datetime4_datatype_map
      case DateTimeFmt1(d, t) => datetime1_datatype_map
      case DateTimeFmt2(d, t) => datetime2_datatype_map
      case DateTimeFmt3(d, t) => datetime3_datatype_map

      case NumberFmt(n) => {
        if(s.toDouble >= 0 && s.toDouble <= 1)
          percentage_datatype_map(s)
        else
          number_datatype_map(s)
      }

      case "" => null_datatype_map
      case _ => text_datatype_map
    }

    def getColumnDatatype(ls: List[String]): Map[String, Any] = {

      val datatype_map_list = ls.map(x => getSingleValueDatatype(x))
      val distinct_datatype_maps = datatype_map_list.distinct
      val predicted_datatypes = distinct_datatype_maps.filter(_ != null_datatype_map)
      val distinct_datatypes = predicted_datatypes.map(_ ("datatype")).distinct.asInstanceOf[List[String]]

      if (predicted_datatypes.size == 0)
        text_datatype_map
      else if (predicted_datatypes.size == 1) {
        if(distinct_datatypes(0) == "Text") {
          if(ls.distinct.size >= min_distinct_values_for_string)
            text_datatype_map
          else
            category_datatype_map
        }
        else if(distinct_datatypes(0) == "Percentage" &&
          predicted_datatypes.map(x => x("decimal").asInstanceOf[Int]).max == 0)
          number_datatype_map("1")
        else
          predicted_datatypes(0)
      }
      else if(distinct_datatypes.size == 1 &&
        (distinct_datatypes(0) == "Number" || distinct_datatypes(0) == "Percentage")) {
        (predicted_datatypes(0) - "decimal") + ("decimal" ->
          (predicted_datatypes.map(x => x("decimal").asInstanceOf[Int]).max))
      }
      else if(distinct_datatypes.size == 2 && distinct_datatypes.sorted == List("Number","Percentage").sorted) {
        (number_datatype_map("1") - "decimal") + ("decimal" ->
          (predicted_datatypes.map(x => x("decimal").asInstanceOf[Int]).max))
      }
      else {
        if(ls.distinct.size >= min_distinct_values_for_string)
          text_datatype_map
        else
          category_datatype_map
      }
    }

    val col_wise_data = data_preview.transpose
    col_wise_data.map(getColumnDatatype(_))
  }

  def getColType(data_type: String): (String, String) = {
    val numeric_type = List("INTEGER","INT","SMALLINT","TINYINT","MEDIUMINT",
      "BIGINT","DECIMAL","NUMERIC","FLOAT","DOUBLE","BIT", "INT UNSIGNED")
    val string_type = List("CHAR","VARCHAR")
    val text_type = List("TINYTEXT","TEXT","MEDIUMTEXT","LONGTEXT")
    val date_type = List("DATE","DATETIME","TIMESTAMP","TIME")

    if(numeric_type.contains(data_type)){
      ("Number", "")
    }else if(text_type.contains(data_type)){
      ("Text", "string")
    }else if(date_type.contains(data_type)){
      val format = data_type match {
        case "DATE" => "YYYY-MM-DD"
        case "TIME" => "HH:MM:SS"
        case "TIMESTAMP" => "YYYYMMDDHHMMSS"
        case _ => "YYYY-MM-DD HH:MM:SS"
      }
      ("Date", format.toString)
    }else{
      ("Category", "")
    }

  }

}
