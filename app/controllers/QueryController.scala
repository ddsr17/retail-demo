package controllers

import java.text.SimpleDateFormat

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{UserDefinedFunction, SQLContext, DataFrame}
import play.api.mvc.{Action, Controller}

import scala.util.control.Breaks

/**
 * Created by sarthak on 28/11/15.
 */
class QueryController extends Controller {
  val conf = new SparkConf().setAppName("capillary").setMaster("local[1]")

  conf.set("spark.shuffle.consolidateFiles", "true")
    .set("spark.rdd.compress", "true")
    .set("spark.shuffle.manager", "sort")

  val sc = new SparkContext(conf)
    sc.addJar("commons-csv-1.2.jar")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  sqlContext.setConf("spark.sql.shuffle.partitions","1")

/*  def dataJoin(df1: DataFrame, df2: DataFrame, joinCol: String): DataFrame = {
    val dataframe1 = df1.withColumnRenamed(joinCol, "joinColdrop")
    val joinDataframa = dataframe1.join(df2, df2.col(joinCol) === dataframe1.col("joinColdrop"), "outer").drop("joinColdrop")
    return joinDataframa
  }

  def dropSingleColumn(df: DataFrame, cName: String): DataFrame = {
    df.drop(cName);
  }

  def dropMultipleColumn(df: DataFrame, cList: List[String]): DataFrame = {
    var dfcopy = df
    for (cName <- cList) {
      dfcopy = dfcopy.drop(cName)
    }
    val dfreturn = dfcopy
    return dfcopy
  }

  def selectColumns(df: DataFrame, cList: List[String]): DataFrame = {
    return df.select(cList(0), cList.drop(1): _*)
  }

  def createVertexAndRelation(df: DataFrame, colList: List[String],combineColList:List[String]): (RDD[(String,Double,String)],RDD[(String,String,String,String,Double,Double,String)]) = {
    //val dfmap = df.map(_.getValuesMap[Any](colList))
    val colList1 = df.schema.fieldNames.toList
    val dfmap = df.map(_.getValuesMap[Any](colList1))
    val dfmap1 = dfmap.map(x => (x.toList))
    val dfmap2 = dfmap1.map(x => {
      x.map(y => {
        var a1=""
        // println("----y----= "+y)
        if(y._1!=null){
          a1 = y._1.toLowerCase()
        }
        else
          a1 = null
        var a2=""
        if(y._2==null){
          a2 = null
        }

        else{
          a2 = y._2.toString.toLowerCase
        }

        (a1,a2)
      })
    })

    val list: RDD[List[(String,String)]] = dfmap2
    val lnc1 = list.map(x => combine1(x, 1)).flatMap(x => x).map(x => (x, 1)).reduceByKey(_ + _)
    val lnc2 = list.map(x => combine2(x,combineColList)).flatMap(x => x).map(x => (x, 1)).reduceByKey(_ + _)
    val tup = lnc2.map(x => {
      val key = x._1
      val value = x._2
      val key1 = key._1
      val key2 = key._2
      val tuple1 = (key1, (key, value))
      val tuple2 = (key2, (key, value))
      (tuple1, tuple2)
    })
    val tuple1 = tup.map(x => x._1)
    val tuple2 = tup.map(x => x._2)
    val join1 = tuple1.leftOuterJoin(lnc1)
    val join2 = tuple2.leftOuterJoin(lnc1)
    val unionjoin = join1.union(join2)
    val res = unionjoin.map(x => {
      val key1 = x._1
      val v1 = x._2
      val keyvaluecombine = v1._1
      val key1freq = v1._2
      val key = keyvaluecombine._1
      val keyfreq = keyvaluecombine._2
      (key, ((key1, key1freq), keyfreq))
    })
    val resfinal = res.map(x => ((x._1, x._2._2), (x._2._1._1, x._2._1._2))).groupByKey()
    val resf2 = resfinal.map(x => (x._1._1, x._1._2, x._2.map(x => (x._1, x._2))))
    val resf3 = resf2.map(x => {
      val x1 = x._1
      val x2 = x._2
      val x3 = x._3
      (x1, x2, x3)
    })
    val nodes1 = lnc1.map(x => {
      val n1 = x._1
      val freq = x._2
      val dimension = n1._1
      val value = n1._2
      val name = n1
      val strValue = (value,freq.toDouble,dimension)
      val dimFreq = 1
      val strDim = (dimension,dimFreq.toDouble,dimension)
      (strValue, strDim)
    })
    val nodesValue = nodes1.map(x => x._1)
    val nodesDim = nodes1.map(x => x._2)
    val nodesUnion = nodesValue.union(nodesDim)
    val nodesUnionNew= nodesUnion.map(x => x._1+","+x._2+","+x._3)
    val relationValueDimension = nodes1.map(x => {
      val bigNode = x._1
      val smallNode = x._2
      val bigNodeName = bigNode._1
      val bigNodeFreq = bigNode._2
      val bigNodeDim = bigNode._3
      val smallNodeName = smallNode._1
      val smallNodeFreq = smallNode._2
      val smallNodeDim = smallNode._3
      val strength = 1.0
      val frequency = 1.0
      val relationType = "relation"
      val r = (bigNodeName,bigNodeDim,smallNodeName,smallNodeDim,frequency,strength,relationType)
      //val r = bigNodeName + "," + smallNodeName + "," + frequency + "," + strength + "," + "relation"
      r
    })
    val resf4 = resf3.map(x => {
      val nR = x._1
      val nRfreq = x._2
      val z = x._3
      var x4="empty"
      var n1dim=""
      var n1val =""
      var n1freq =""
      var n2dim =""
      var n2val = ""
      var n2freq =""
      for (y <-z) {
        if(x4.equals("empty")){
          n1dim = y._1._1
          n1val = y._1._2
          n1freq = y._2.mkString
          x4="notempty"
        }
        else{
          n2dim = y._1._1
          n2val = y._1._2
          n2freq = y._2.mkString
          x4="notempty"
        }

      }
      //n1dim.equals(combineName)
      // combineName.contains(n1dim)

      val strength = try {
        (nRfreq.toDouble / (n1freq.toDouble * n2freq.toDouble))
      } catch  {
        case e:Exception => 0
      }
      val relationType = "relation"
      if (combineColList.contains(n1dim)) {
        // println("--------------n1dim If------------"+n1dim)
        // val r = n1val + "," + n2val + "," + nRfreq + "," + (nRfreq.toDouble / (n1freq.toDouble * n2freq.toDouble)) + "," + "relation"
        val r = (n1val,n1dim,n2val,n2dim,nRfreq.toDouble,strength,relationType)
        r
      }
      else {
        // println("----------nidim ELse-----------"+n2dim)
        // val r = n2val + "," + n1val + "," + nRfreq + "," + (nRfreq.toDouble / (n1freq.toDouble * n2freq.toDouble)) + "," + "relation"
        val r = (n2val,n2dim,n1val,n1dim,nRfreq.toDouble,strength,relationType)
        r
      }
    })
    val relationUnion = resf4.union(relationValueDimension)
    val relationUnionNew = relationUnion.map(x=> x._1 +","+x._2+","+x._3+","+x._4+","+x._5+","+x._6+","+x._7)
    (nodesUnion, relationUnion)
  }

  def combine1(in: List[(String,String)], length: Int): Seq[(String,String)]={
    return  in
  }

  def combine2(in: List[(String,String)], inStr: List[String]): Seq[((String,String),(String,String))] = {
    var out: Seq[((String,String),(String,String))] = Seq[((String,String),(String,String))]()
    for(inStrElem<-inStr){
      var strMerge = ("","")
      val loop = new Breaks
      loop.breakable {
        for (s <- in) {

          if (s._1.equals(inStrElem.toLowerCase)) {
            strMerge = s
            loop.break()
          }
        }
      }
      for (str <- in) {
        if (!str.equals(strMerge)) {
          val a = (strMerge, str)
          out= out :+ a
        }
      }

    }
    return out

  }
  def getAdjacencyList(vertex:RDD[(String,Double,String)],relation:RDD[(String,String,String,String,Double,Double,String)]):RDD[(((String,String),Double),List[(String,String,Double,Double,String)],List[(String,String,Double,Double,String)])]={
    val edges = relation
    val oEdges = edges.map(x =>{
      ((x._1,x._2),List((x._3,x._4,x._5,x._6,x._7)))
    })
    val iEdges = edges.map(x=>{
      ((x._3,x._4),List((x._1,x._2,x._5,x._6,x._7)))
    })
    val oREdges = oEdges.reduceByKey(_++_).map(x=>{
      val vertex= x._1
      val edgeList = x._2
      (vertex,edgeList)
    })
    val iREdges = iEdges.reduceByKey(_++_).map(x=>{
      val vertex = x._1
      val edgeList = x._2
      (vertex,edgeList)
    })
    val vertices= vertex.map(x=>{
      ((x._1,x._3),x._2)
    })
    val adjList1 = vertices.join(oREdges)
    val adjList2 = adjList1.join(iREdges)
    val adjListPrintable = adjList2.map(x=>{
      val v1 = x._1
      val v2 = x._2._1._1
      val outE = x._2._1._2
      val inE = x._2._2
      ((v1,v2),outE,inE)
    })
    /*val adjListPrintable = adjList2.map(x=>{
      x._1._1+"###&"+x._1._2+"##&"+x._2._1._1+"::"+x._2._1._2.mkString("|")+"::"+x._2._2.mkString("|")
    })*/
    // RDD[(((String,String),Double),List[(String,String,Double,Double,String)],List[(String,String,Double,Double,String)])]
    adjListPrintable
  }

  def getRFM(colNameDate:String,colNameAmount:String,customer:String,df:DataFrame,sQLContext: SQLContext):DataFrame= {
    val unixcoder = getUnixTime()
    val rdf = df.withColumn(colNameDate + "_unixtime", unixcoder(col(colNameDate)))
    val groupedDf = rdf.groupBy(customer)
    val recency = groupedDf.max(colNameDate + "_unixtime").withColumnRenamed("MAX("+colNameDate+"_unixtime)", "recencycreated")
    val freqDF: DataFrame = getFrequency(rdf, customer, colNameDate+"_unixtime",sQLContext)
    val freqCoder = getDateFromUnix()
    val freqdfDate = freqDF.withColumn("frequencycreated",freqCoder(col("value")))
    val freqdfRenamed = freqdfDate.withColumnRenamed("key",customer).drop("value")

    val toDoubleConvert = toDoubleConvertUDF()                //udf[Double, String](_.toDouble)
    val doubleDf = df.withColumn(colNameAmount + "_moneycalc", toDoubleConvert(col(colNameAmount)))
    val groupedDf1 = doubleDf.groupBy(customer)

    val totalM = groupedDf1.sum(colNameAmount + "_moneycalc").withColumnRenamed("SUM("+colNameAmount+"_moneycalc)","totalmoneyspent")
    val AvgM = groupedDf1.avg(colNameAmount + "_moneycalc").withColumnRenamed("AVG("+colNameAmount +"_moneycalc)","avgmoneyspent")

    val dfjoin1 = dataJoin(totalM,recency,customer)
    val dfjoin2 = dataJoin(dfjoin1,AvgM,customer)
    val dfjoin3 = dataJoin(dfjoin2,freqdfRenamed,customer)
    val dfjoin4 = dataJoin(df,dfjoin3,customer)

    dfjoin4
  }

  def toDoubleConvertUDF():UserDefinedFunction={
    val coder: (String=>Double) = (arg:String) => {
      var a = 0.0
      if(arg==null || arg.isEmpty)
        a = 0.0
      else
        a = arg.toDouble
      a
    }
    udf(coder)
  }

  def getUnixTime():UserDefinedFunction = {
    val coder: (String=>Long) = (arg:String) => {
      val dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = dfm.parse(arg).getTime
      time
    }
    udf(coder)
  }

  def getDateFromUnix():UserDefinedFunction = {
    val coder: (Double=>String) = (arg:Double) => {

      /* val date = new Date(arg.toLong)
       val dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
       dfm.setTimeZone(TimeZone.getTimeZone("GMT-4"))
       val date1 = dfm.format(date)
       val days = date.getTime/(24 * 60 * 60 * 1000)*/
      //println("-----------------date-------------"+date1)
      //date1
      /*val date2 = date.getTime
      date2.toString*/
      arg.toLong.toString
      //days.toString
    }
    udf(coder)
  }

  case class FreqData(key: String, value: Double)
  def getFrequency(df: DataFrame, keyColumn: String, freqColumn: String, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val keyIndex = df.schema.fieldIndex(keyColumn)
    val freqIndex = df.schema.fieldIndex(freqColumn)
    val pairRDD = new PairRDDFunctions(df.map(row => (row(keyIndex).toString, row(freqIndex).toString.toLong)))
    val rdd = pairRDD.groupByKey()
    val r2: RDD[(String,Double)] = rdd.map({case (k, v) =>
      val vList = v.toList
      val newList = (vList drop 1, vList).zipped.map(_ - _)
      val avg: Double = if(newList.size != 0) newList.reduce(_ + _).toDouble/newList.size.toDouble else 0.0
      (k, avg)
    })
    r2.map(pair => FreqData(pair._1, pair._2)).toDF
  }

  def getExpansion(fromColumnList:List[String],toColumnsList:List[String],df:DataFrame):RDD[(String,String,String,String,Double,Double,String)]= {
    val list = fromColumnList ::: toColumnsList
    val df1 = selectColumns(df,list)
    val colHeaders = df1.schema.fieldNames.toList
    val rddVertexRelation = createVertexAndRelation(df1,colHeaders,fromColumnList)
    rddVertexRelation._2
  }*/

  def sparkJobBuilder(fromColumnList:List[String],toColumnsList:List[String])= Action {

    println(fromColumnList+" "+toColumnsList)

    println("----After sc-----")

//    val array = Seq(1, 2, 3, 4, 5, 6)
//    val count = sc.parallelize(array, 3).count()
  //  val dfBill = sc.textFile("Bill10.csv").map(_.split(","))
    val dfBill = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("Bill10.csv")
    //   val dfLine = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("Line10.csv")
    // dfBill.show()
    Ok("")
  }
}
