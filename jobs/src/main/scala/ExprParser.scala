import scala.util.parsing.combinator._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class ExpressionParser extends JavaTokenParsers {

  type C = Column

  //Helper functions
  var df: DataFrame = SparkConfig.getSparkSession.emptyDataFrame

  def getColumn(x: String): C = df(x)

  def getMaximumValue(x: C) = df.agg(max(x)).collect().map(r => r.toSeq(0).asInstanceOf[Double]).toList(0)
  def getMinimumValue(x: C) = df.agg(min(x)).collect().map(r => r.toSeq(0).asInstanceOf[Double]).toList(0)
  def getMeanValue(x: C) = df.agg(mean(x)).collect().map(r => r.toSeq(0).asInstanceOf[Double]).toList(0)
  def getStddevValue(x: C) = df.agg(stddev(x)).collect().map(r => r.toSeq(0).asInstanceOf[Double]).toList(0)

  def addColumns(x: List[C]): C = x.reduce(_ + _)
  def subColumns(x: List[C]): C = lit(0).minus(x.reduce(_ + _))
  def normalizeColumn(x: C, y: Double, z: Double): C = x.expr.dataType.typeName match {
    case "integer" | "double" => {
      val max_x = getMaximumValue(x.cast("double"))
      val min_x = getMinimumValue(x.cast("double"))
      lit(y) + (x.cast("double") - lit(min_x)) * (lit(z) - lit(y)) / (lit(max_x) - lit(min_x))
    }
    case _ => lit(null)
  }

  def replaceOutlierColumn(x: C, y: C) = x.expr.dataType.typeName match {
    case "integer" | "double" => ifThenElse(getOutlierColumn(x), y, x)
    case _ => lit(null)
  }

  def getOutlierColumn(x: C) = x.expr.dataType.typeName match {
    case "integer" | "double" => {
      val mean_x = getMeanValue(x.cast("double"))
      val stddev_x = getStddevValue(x.cast("double"))
      val number_of_stddevs = 3
      val isoutlier = abs(x - lit(mean_x)) > lit(number_of_stddevs * stddev_x)
      ifThenElse(isoutlier, lit(true), lit(false))
    }
    case _ => lit(null)
  }

  def trimColumn(x: C): C = trim(x)
  def concatenateColumns(x: List[C]): C = concat(x.map(c => c): _*)
  def replaceColumn(e: C, pattern: String, replacement: String) = regexp_replace(e, pattern, replacement)

  def minColumns(x: List[C]): C = (x.head /: x.tail) ((min, c) => {
    val min_col = (min - c) < lit(0)
    min_col.cast("double") * min + (lit(1) - min_col.cast("double")) * c
  })

  def maxColumns(x: List[C]): C = (x.head /: x.tail) ((max, c) => {
    val max_col = (max - c) > lit(0)
    max_col.cast("double") * max + (lit(1) - max_col.cast("double")) * c
  })

  def andColumns(x: List[C]): C = x.reduce((a, b) => a.cast("boolean") && b.cast("boolean"))
  def orColumns(x: List[C]): C = x.reduce((a, b) => a.cast("boolean") || b.cast("boolean"))
  def notColumn(x: C): C = !(x.cast("boolean"))
  def likeColumn(x: C, y: C) = x.contains(y)
  def inColumn(x: C, y: List[C]) = x.isin(y: _*)
  def isNullColumn(x: C): C = isnull(x)
  def ifNullColumn(x: C, y: C) = {
    val is_null = isnull(x)
    when(is_null, y).otherwise(x)
  }

  def ifThenElse(x: C, y: C, z: C): C = when(x, y).when(!x, z)
  def caseWhenThen(x: C, y: List[(C, C)]): C = y.foldLeft(lit(null))((a, e) => when(x.equalTo(e._1), e._2).otherwise(a))

  def greaterThanOrEqualTo(x: C, y: C): C = x >= y
  def lessThanOrEqualTo(x: C, y: C): C = x >= y
  def greaterThan(x: C, y: C): C = x > y
  def lessThan(x: C, y: C): C = x < y
  def isEqualTo(x: C, y: C): C = x.equalTo(y)
  def notEqualTo(x: C, y: C): C = x.notEqual(y)

  //Complex expression parsing
  def expr: Parser[C] = sumdiffterm ~ rep(relational_expr) ^^ { case a ~ b => (a /: b) ((acc, f) => f(acc)) }
  def sum: Parser[C => C] = "+" ~ cmplxterm ^^ { case "+" ~ b => _ + b }
  def diff: Parser[C => C] = "-" ~ cmplxterm ^^ { case "-" ~ b => _ - b }
  def sumdiffterm: Parser[C] = cmplxterm ~ rep(sum | diff) ^^ { case a ~ b => (a /: b) ((acc, f) => f(acc)) }
  def cmplxterm: Parser[C] = smplterm ~ rep(prod | div | mod) ^^ { case a ~ b => (a /: b) ((acc, f) => f(acc)) }
  def prod: Parser[C => C] = "*" ~ smplterm ^^ { case "*" ~ b => _ * b }
  def div: Parser[C => C] = "/" ~ smplterm ^^ { case "/" ~ b => _ / b }
  def mod: Parser[C => C] = "%" ~ smplterm ^^ { case "%" ~ b => _ % b }
  def smplterm: Parser[C] = singleterm ~ rep(power) ^^ { case a ~ b => (a /: b) ((acc, f) => f(acc)) }
  def power: Parser[C => C] = "^" ~ singleterm ^^ { case "^" ~ b => pow(_, b) }
  def singleterm: Parser[C] = "(" ~> expr <~ ")" | term

  //Relational expression parsing
  def relational_expr: Parser[C => C] =
    ">=" ~ sumdiffterm ^^ { case ">=" ~ y => greaterThanOrEqualTo(_: C, y) } |
      "<=" ~ sumdiffterm ^^ { case "<=" ~ y => lessThanOrEqualTo(_: C, y) } |
      ">" ~ sumdiffterm ^^ { case ">" ~ y => greaterThan(_: C, y) } |
      "<" ~ sumdiffterm ^^ { case "<" ~ y => lessThan(_: C, y) } |
      "==" ~ sumdiffterm ^^ { case "==" ~ y => isEqualTo(_: C, y) } |
      "!=" ~ sumdiffterm ^^ { case "!=" ~ y => notEqualTo(_: C, y) } |
      "<>" ~ sumdiffterm ^^ { case "!=" ~ y => notEqualTo(_: C, y) }

  //Simple term parsing
  def term: Parser[Column] = num_func |
    string_func |
    boolean_func |
    string_col |
    "[" ~> col_name <~ "]" |
    fpn_col

  def num_func = "SUM(" ~> comma <~ ")" ^^ { case x => addColumns(x) } |
    "MIN(" ~> comma <~ ")" ^^ { case x => minColumns(x) } |
    "MAX(" ~> comma <~ ")" ^^ { case x => maxColumns(x) } |
    "CEIL(" ~> expr <~ ")" ^^ { case x => ceil(x) } |
    "FLOOR(" ~> expr <~ ")" ^^ { case x => floor(x) } |
    "ABS(" ~> expr <~ ")" ^^ { case x => abs(x) } |
    "LOG(" ~> expr <~ ")" ^^ { case x => log10(x) } |
    "LN(" ~> expr <~ ")" ^^ { case x => org.apache.spark.sql.functions.log(x) } |
    "MOD(" ~> expr ~ "," ~ expr <~ ")" ^^ { case x ~ "," ~ y => pmod(x, y) } |
    "POW(" ~> expr ~ "," ~ expr <~ ")" ^^ { case x ~ "," ~ y => pow(x, y) } |
    "ROUND(" ~> expr <~ ")" ^^ { case x => round(x) } |
    "SQRT(" ~> expr <~ ")" ^^ { case x => sqrt(x) } |
    "NORMALIZE(" ~> expr ~ "," ~ floatingPointNumber ~ "," ~ floatingPointNumber <~ ")" ^^ { case x ~ "," ~ y ~ "," ~ z =>
      normalizeColumn(x, y.toDouble, z.toDouble)
    } |
    "ISOUTLIER(" ~> expr <~ ")" ^^ { case x => getOutlierColumn(x)} |
    "REPLACEOUTLIER(" ~> expr ~ "," ~ expr <~ ")" ^^ { case x ~ "," ~ y => replaceOutlierColumn(x, y)}

  def string_func = "TRIM(" ~> expr <~ ")" ^^ { case x => trimColumn(x) } |
    "CONCAT(" ~> comma <~ ")" ^^ { case x => concatenateColumns(x) } |
    "FIND(" ~> expr ~ "," ~ string <~ ")" ^^ { case x ~ "," ~ y => instr(x, y) } |
    "REPLACE(" ~> expr ~ "," ~ string ~ "," ~ string <~ ")" ^^ { case x ~ "," ~ y ~ "," ~ z =>
      replaceColumn(x, y, z)
    } |
    "LEFT(" ~> expr ~ "," ~ integer <~ ")" ^^ { case x ~ "," ~ y => substring(x, 0, y) } |
    "RIGHT(" ~> expr ~ "," ~ integer <~ ")" ^^ { case x ~ "," ~ y => regexp_extract(x, "(.{1,"+y+"}$)", 1) } |
    "LENGTH(" ~> expr <~ ")" ^^ { case x => length(x) } |
    "LOWER(" ~> expr <~ ")" ^^ { case x => lower(x) } |
    "UPPER(" ~> expr <~ ")" ^^ { case x => upper(x) } |
    "SUBSTR(" ~> expr ~ "," ~ integer ~ "," ~ integer <~ ")" ^^ { case x ~ "," ~ y ~ "," ~ z => substring(x, y, z) }

  def boolean_func = "AND(" ~> comma <~ ")" ^^ { case x => andColumns(x) } |
    "OR(" ~> comma <~ ")" ^^ { case x => orColumns(x) } |
    "NOT(" ~> expr <~ ")" ^^ { case x => notColumn(x) } |
    "LIKE(" ~> expr ~ "," ~ expr <~ ")" ^^ { case x ~ "," ~ y => likeColumn(x, y) } |
    "IN(" ~> expr ~ ",{" ~ comma <~ "})" ^^ { case x ~ ",{" ~ y => inColumn(x, y) } |
    "ISNULL(" ~> expr <~ ")" ^^ { case x => isNullColumn(x) } |
    "IFNULL(" ~> expr ~ "," ~ expr <~ ")" ^^ { case x ~ "," ~ y => ifNullColumn(x, y) } |
    "IF" ~> expr ~ "THEN" ~ expr ~ "ELSE" ~ expr <~ "END" ^^ { case x ~ "THEN" ~ y ~ "ELSE" ~ z =>
      ifThenElse(x, y, z)
    } |
    "CASE" ~> expr ~ rep(whenThen) <~ "END" ^^ { case x ~ y => caseWhenThen(x, y) }

  def comma: Parser[List[C]] = repsep(expr, ",")
  def string_col: Parser[C] = string ^^ { case x => lit(x) }
  def col_name: Parser[C] = "[^]]*".r ^^ { case x => getColumn(x) }
  def string: Parser[String] = "'" ~> "[^\']*".r <~ "'" ^^ { case x => x.toString }
  def whenThen: Parser[(C, C)] = "WHEN" ~ expr ~ "THEN" ~ expr ^^ { case "WHEN" ~ x ~ "THEN" ~ y => (x, y) }
  def fpn_col: Parser[C] = floatingPointNumber ^^ { case x => lit(x) }
  def integer: Parser[Int] = floatingPointNumber ^^ { case x => x.toInt }
}

object ExprParser extends ExpressionParser {

  def calculator(dataframe: DataFrame, col_name: String, input: String, expr: Parser[Any] = expr) = {
    df = dataframe
    parseAll(expr, input) match {
      case Success(result, _) => Some(df.withColumn(col_name, lit(result)))
      case NoSuccess(_, _) => None
    }
  }
}