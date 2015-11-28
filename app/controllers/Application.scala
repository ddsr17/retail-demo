package controllers

import java.io.{BufferedOutputStream, FileOutputStream}
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder
import java.text.SimpleDateFormat
;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions._
import play.api._
import play.api.data.Form
import scala.concurrent.{Await, Future}

import play.api.libs.json._
import play.api.mvc.AnyContent
import play.api.libs.ws.{WS, WSRequest, WSClient}
import play.api.mvc._
import play.api.Play.current
import scala.concurrent.{Future, Await}
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import play.api.libs.ws.WS
import play.api.libs.functional.syntax._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.Breaks
import scala.util.{Try, Failure, Success}
import scala.concurrent._
import ExecutionContext.Implicits.global


case class entityobj(name:String,vFrequency: String,strength:String, frequency: String)
case class childobj(id: String,strength: String,frequency: String)
case class newentity(id: String,name:String,vFrequency: String,strength:String, frequency: String)

class Application extends Controller {

  def options(path:String) = Action { Ok("")}

  def index = Action {
    Ok(views.html.index("Your new application is ready.", Nil,"",-1))
  }

  def display1 = Action {
    Ok(views.html.rohith("Your new application is ready.",("",Nil),"",0))
  }

  def display2 = Action {
    Ok(views.html.ved("Your new application is ready.",("",Nil),"",0))
  }

  def display3 = Action {
    Ok(views.html.sarthak())
  }

  import play.api.data.Forms._
  val newsForm = Form(
    single("news" -> text)
  )

//  def convertEntToJsonOrig(ent: Seq[entityobj]): JsValue = {
//    Json.toJson(
//      ent.map { t =>
//        Map("name" -> t.name, "VFrequency" -> t.vFrequency, "strength" -> t.strength, "EFrequency" -> t.frequency)
//      }
//    )
//  }
  //(item: String,fFilter: Double)

  def getVertices1(item: String,fFilter: Double) = Action.async {implicit request =>
    //    val newsItem = newsForm.bindFromRequest().get
    val newsitem = item.toLowerCase()
    val newsItem = URLEncoder.encode(newsitem,"UTF-8")
    var freq = ""

    //    val forOutput1 = for {
    //      a <- WS.url("http://localhost:8282/graphs/titanconnected/vertices?key=name&value=" + newsItem).get()
    //      b = (a.json \ "results").as[JsArray].value.head
    //      c = (b \ "frequency").asOpt[String]
    //      d = (b \ "_id").asOpt[String]
    //      e = d match {
    //        case None => Seq.empty[entityobj]
    //        case Some(id) =>
    //          val out = for {
    //          e <- WS.url("http://107.167.178.97:8282/graphs/titanconnected/vertices/" + id + "/outE?_label=Relation").get()
    //          f <- (e.json \ "results").as[JsArray].value
    //          //          f1 <- ((f \ "frequency").as[Double], (f \ "strength").as[String])
    //          g <- WS.url("http://107.167.178.97:8282/graphs/titanconnected/vertices/" + id + "/outE?_label=To").get()
    //          h <- (g.json \ "results").as[JsArray].value
    //          //          h = ((g.json \ "results" \ "frequency").as[Double], (g.json \ "results" \ "strength").as[String])
    //          k <- WS.url("http://107.167.178.97:8282/graphs/titanexample/vertices/" + (f \ "_inv").as[String]).get()
    //          m = (k.json \ "results").as[JsArray].value.head
    //          l <- WS.url("http://107.167.178.97:8282/graphs/titanexample/vertices/" + (h \ "_inv").as[String]).get()
    //          n = (l.json \ "results").as[JsArray].value.head//
    //          o = entityobj((m \ "name").as[String], (m \ "frequency").as[String], (f \ "strength").as[String], (f \ "frequency").as[Double])
    //          p = entityobj((n \ "name").as[String], (n \ "frequency").as[String], (h \ "strength").as[String], (h \ "frequency").as[Double])
    //        } yield (Seq(o, p))
    //      }
    //    } yield (c, e)
    //
    //    for {
    //      res1 <- forOutput1
    //      a = res1 match {
    //        case N
    //      }
    //    }
    //
    //    match {
    //      case None => None
    //      case Some(id) => for {
    //
    //      }
    //        u =
    //          c  b.map(res => (res \\ "frequency")).toString()
    //      d = b.map(res => (res \ "_id").get.toString()) match {
    //        case Nil => None
    //        case string: Seq[String] => Some(string.head)
    //      }
    //      e <- WS.url("http://107.167.178.97:8282/graphs/titanconnected/vertices/" + d.get + "/outE?_label=Relation").get()
    //      f = ((e.json \ "results" \ "frequency").as[Double], (e.json \ "results" \ "strength").as[String])
    //      g <- WS.url("http://107.167.178.97:8282/graphs/titanconnected/vertices/" + d.get + "/outE?_label=To").get()
    //      h = ((g.json \ "results" \ "frequency").as[Double], (g.json \ "results" \ "strength").as[String])
    //      k <- WS.url("http://107.167.178.97:8282/graphs/titanexample/vertices/" + (e.json \ "results" \ "_inv").as[String]).get()
    //      m = ((k.json \ "results" \ "name").as[String], (k.json \ "results" \ "frequency").as[String])
    //      l <- WS.url("http://107.167.178.97:8282/graphs/titanexample/vertices/" + (g.json \ "results" \ "_inv").as[String]).get()
    //      n = ((l.json \ "results" \ "name").as[String], (l.json \ "results" \ "frequency").as[String])
    //      o = entityobj(m._1, m._2, f._2, f._1)
    //      p = entityobj(n._1, n._2, h._2, h._1)
    //    } yield (o, p)
    //    forO
    //: Future[Option[String]]
    val futureResult = WS.url("http://40.124.54.95:8182/graphs/titanexample/vertices?key=name&value=" + newsItem).get().map {
      response => {
        val temp = (response.json \ "results").as[JsArray].value
        temp match {
          case Nil => {
            freq = ""
          }
          case a => {
            freq = (temp.head \ "frequency").get.toString
          }
        }

        val id = temp.map(res => (res \ "_id").get.toString()) match {
          case Nil => None
          case string: Seq[String] => Some(string.head)
        }
        (id,freq)
      }
    }

    var data: Seq[Seq[JsValue]] = Seq()

    val future: Future[Seq[Seq[JsValue]]] = futureResult.map(x=> x._1).flatMap {

      case None => Future(Seq[Seq[JsValue]]())
      //case None => Future(Seq[JsValue]())
      case Some(a) =>

        val finaldata = {

          val output = WS.url("http://40.124.54.95:8182/graphs/titanexample/vertices/" + a + "/outE?_label=relation").get().map {
            result => {
              (result.json \ "results").as[JsArray].value
            }
          }

          val outputtoLabel = WS.url("http://40.124.54.95:8182/graphs/titanexample/vertices/" + a + "/outE?_label=To").get().map{
            result => {
              (result.json \ "results").as[JsArray].value
            }
          }

          val relationseq = output.map{x => {
            data = data :+ x
            //println(data)
            data
          }
          }

          val bothseq = relationseq.flatMap(count => {
            val gotonevalue = outputtoLabel.map{y => {
              data = data :+ y
              data
            }
            }
            gotonevalue
          })


          //        val either1 = output.map {
          //          case e: JsResultException => {
          //            Left(e)
          //            Redirect(routes.Application.index)
          //                        Redirect("/")
          //            Ok(views.html.index("Your new application is ready.", Nil, "", -1))
          //          }
          //          case result: Seq[JsValue] =>
          //            Right(result)
          //        }

          //          val recover = output.recover{
          //            case e: JsResultException =>
          //              None
          //            //Redirect("http://google.com")
          //
          //            case js: Future[Seq[JsValue]] => Some(js)
          //          }

          bothseq
        }
        finaldata
    }

    val response2 = future.map(
      obj => {
        val check = obj.map(error1 => {

          error1.map(error2 => {
            val strength = (error2 \ "strength").get.toString()
            val frequency = (error2 \ "frequency").get.toString().toDouble
            val inv = (error2 \ "_inV").get.toString()

            WS.url("http://40.124.54.95:8182/graphs/walmart/vertices/" + inv + "/outE").get().map(itr =>{
              (itr.json \ "results").as[JsArray].value
            })

            val tuple = (inv,(frequency,strength))

            tuple
          })
        })
        //check.foreach().groupBy()

        val filteredresults = check.map(temp => {
          val   afilter = temp.filter(x => {
            x._2._1 > fFilter
          })

          val results = afilter.take(10)

          results
        })

        //      val filteredresults = check.filter(temp => {
        //        temp._2._1 > fFilter
        //      })

        //val take = filteredresults.take(100)
        //take

        filteredresults
      })


    val now = response2.flatMap{ x => {
          //   println(x)
      val dataToSend = x.map { temp => {
        val necessary = temp.map(key => {

          val getName = key._1
          val efreq = key._2._1.toString()

          //println(key._1)

          val check = WS.url("http://40.124.54.95:8182/graphs/titanexample/vertices/" + getName).get().map {
            res => {
              val temp =  (res.json \ "results").get
              val name = (temp \ "name").get.toString()
              val vertexFrequency = (temp \ "frequency").get.toString()

              (name,vertexFrequency)
            }
          }

          val finalTuple = check.map(name => entityobj(name._1,name._2, key._2._2, efreq))
          finalTuple
        })

       Future.sequence(necessary)
      }
      }
      Future.sequence(dataToSend)
    }
    }


    val htmlInput = for {
      fut1 <- futureResult
      fut2 <- now
    //res1 <- fut1._2
    } yield (fut1._2,fut2)

    htmlInput.map {
      case result: (String,Seq[Seq[entityobj]]) =>
        Ok(views.html.rohith("Showing data in table",result,item,fFilter))
    }
  }

  def getVertices2(item: String,fFilter: Double) = Action.async {implicit request =>
    //    val newsItem = newsForm.bindFromRequest().get
    val newsItem = item.toLowerCase()
    //val newsItem = URLEncoder.encode(item,"UTF-8")
    var freq = ""

    val futureResult = WS.url("http://40.124.54.95:8182/graphs/titanconnected/vertices?key=name&value=" + newsItem).get().map {
      response => {
        val temp = (response.json \ "results").as[JsArray].value
        freq = (temp.head \ "frequency").get.toString
        val id = temp.map(res => (res \ "_id").get.toString()) match {
          case Nil => None
          case string: Seq[String] => Some(string.head)
        }
        val both = (id,freq)
        (id,freq)
      }
    }

    var data: Seq[Seq[JsValue]] = Seq()

    val future: Future[Seq[Seq[JsValue]]]= futureResult.map(x => x._1).flatMap {

      case None => Future(Seq[Seq[JsValue]]())
      //case None => Future(Seq[JsValue]())
      case Some(a) =>

        val finaldata = {

          val output = WS.url("http://40.124.54.95:8182/graphs/titanconnected/vertices/" + a + "/outE?_label=Relation").get().map {
            result => {
              (result.json \ "results").as[JsArray].value
            }
          }

          val outputtoLabel = WS.url("http://40.124.54.95:8182/graphs/titanconnected/vertices/" + a + "/outE?_label=To").get().map{
            result => {
              (result.json \ "results").as[JsArray].value
            }
          }

          val relationseq = output.map{x => {
            data = data :+ x
            //println(data)
            data
          }
          }

          val bothseq = relationseq.flatMap(count => {
            val gotonevalue = outputtoLabel.map{y => {
              data = data :+ y
              data
            }
            }
            gotonevalue
          })


          //        val either1 = output.map {
          //          case e: JsResultException => {
          //            Left(e)
          //            Redirect(routes.Application.index)
          //                        Redirect("/")
          //            Ok(views.html.index("Your new application is ready.", Nil, "", -1))
          //          }
          //          case result: Seq[JsValue] =>
          //            Right(result)
          //        }

//          val recover = output.recover{
//            case e: JsResultException =>
//              None
//            //Redirect("http://google.com")
//
//            case js: Future[Seq[JsValue]] => Some(js)
//          }

          bothseq
        }
        finaldata
    }

    val response2 = future.map(
      obj => {
        val check = obj.map(error1 => {

          error1.map(error2 => {
            val strength = (error2 \ "strength").get.toString()
            val frequency = (error2 \ "frequency").get.toString().toDouble
            val inv = (error2 \ "_inV").get.toString()

            val tuple = (inv,(frequency,strength))

            tuple
          })
        })
        val newcheck = check.map(x => {
          val y = x.groupBy(_._1)

          val z = y.map{
            case (k,v) => (v.head)
          }.toSeq
          z
        })

        val filteredresults = newcheck.map(temp => {
          val   afilter = temp.filter(x => {
            x._2._1 > fFilter
          })

          val results = afilter.take(10)

          results
        })
        filteredresults
      })


    val now = response2.flatMap{ x => {
      val dataToSend = x.map { temp => {
        val necessary = temp.map(key => {

          val getName = key._1
          val efreq = key._2._1.toString()

          val check = WS.url("http://40.124.54.95:8182/graphs/titanconnected/vertices/" + getName).get().map {
            res => {
              val temp =  (res.json \ "results").get
              val name = (temp \ "name").get.toString()
              val vertexFrequency = (temp \ "frequency").get.toString()

              (name,vertexFrequency)
            }
          }

          val finalTuple = check.map(name => entityobj(name._1,name._2, key._2._2, efreq))
          finalTuple
        })

        Future.sequence(necessary)
      }
      }
      Future.sequence(dataToSend)
    }
    }

    val htmlInput = for {
      fut1 <- futureResult
      fut2 <- now
    //res1 <- fut1._2
    } yield (fut1._2,fut2)

    htmlInput.map {
      case result: (String,Seq[Seq[entityobj]]) =>
        Ok(views.html.ved("Showing data in table",result,item,fFilter))
    }
  }

  var itr = 1
  var accumulate :Map[String,JsValue] = Map()
  var allInv : Seq[String] = Seq()
  val nResults = 10

  def getVertices3 = Action.async(BodyParsers.parse.json) { implicit request =>
    val tt = request.body

    val item = (tt \ "news").get.toString().replaceAll("^\"|\"$", "")
    val fFilter = (tt \ "frequencyfilter").get.toString().replaceAll("^\"|\"$", "").toDouble

    val newsItem = item.toLowerCase()
    var freq = ""
    //    val newsItem = newsForm.bindFromRequest().get
    println(newsItem)

    val futureResult = WS.url("http://40.124.54.95:8182/graphs/walmart/vertices?key=name&value=" + newsItem).get().map {
      response => {
        val temp = (response.json \ "results").as[JsArray].value

        temp match {
          case Nil => {
            freq = ""
          }
          case a => {
            freq = (temp.head \ "frequency").get.toString
          }
        }

//        val id = temp.map(res => (res \ "_id").get.toString()) match {
//          case Nil => None
//          case string: Seq[String] => Some(string.head)
//        }

        val id = (temp.head \ "_id").get.toString()
        //intln("id " + id)
        (id, freq)
      }
    }

    var secondInv: Seq[String] = Seq()
    var thirdInv: Seq[Seq[String]] = Seq()

    val future = futureResult.flatMap{a =>{

      var data: Seq[Seq[JsValue]] = Seq()
      //case None => Future(Seq[JsValue]())

        val output = WS.url("http://40.124.54.95:8182/graphs/walmart/vertices/" + a._1 + "/outE?_label=relation&_take=").get().map {
          result => {
            (result.json \ "results").as[JsArray].value
          }
        }

        val response2 = output.map(
          obj => {
            val check = obj.map(error1 => {
              val strength = (error1 \ "strength").get.toString()
              val frequency = (error1 \ "frequency").get.toString().toDouble
              val inv = (error1 \ "_inV").get.toString()
              allInv = allInv :+ inv
              val tuple = (inv, (frequency, strength))

              tuple
            })

            //check.foreach().groupBy()

            val afilter = check.filter(x => {
              x._2._1 > fFilter
            })

            var results: Seq[(String, (Double, String))] = Seq()
            if (fFilter == -1) {
              results = afilter.take(nResults)
            }
            else {
              results = afilter.take(nResults)
            }
            results
          })

        val now = response2.flatMap { x => {
          val qString = allInv.mkString(",")
          val getallnames = WS.url("http://40.124.54.95:8182/graphs/walmart/tp/batch/vertices?values=[" + qString + "]").get().map {
            result => {
              val tmp = (result.json \ "results").as[JsArray].value
              val gettuple = tmp.map(x => {
                val name = (x \ "name").get.toString()
                val frequency = (x \ "frequency").get.toString()
                val tuple = (name, frequency)
                tuple
              })
              gettuple
            }
          }

          val putintuple = getallnames.map { itr => {
            val y = itr.zip(x)
            val rr = y.map(z => {
              val efreq = z._2._2._1.toString
              newentity(z._2._1, z._1._1, z._1._2, z._2._2._2, efreq)
            })
            rr
          }
          }

          println("hello")

          val gettingdata = x.map(d => {
            var seq: Seq[String] = Seq()
            val parentid = d._1


            val child = WS.url("http://40.124.54.95:8182/graphs/walmart/vertices/" + parentid + "/outE").get().map(res => {
              (res.json \ "results").as[JsArray].value
            })

            val childata = child.map(x => {
              //            x.map(res => {
              //              val inv = (res \ "_id").get.toString()
              //              secondInv = secondInv :+ inv
              //            })
              val changetoobj = x.map(k => {
                val str = (k \ "strength").get.toString()
                val frq = (k \ "frequency").get.toString()
                val inid = (k \ "_inV").get.toString()

                childobj(inid, str, frq)
              })

              val y = changetoobj.take(3)
              y
            })

            //          var states = scala.collection.mutable.Map[String, Any]()
            //
            //          val composite = childata.map(x => {
            //            val pp = putintuple.map(z => {
            //              val tt = z.map(y => {
            //                val gg =childata.map(i => {
            //                  //states += ()
            //                  val pp = Map("parent" -> parentid,"parentdata" -> y,"childata" -> i)
            //                  pp
            //                })
            //                gg
            //              })
            //              Future.sequence(tt)
            //            })
            //            pp
            //          })
            childata
          })

          //        WS.url("http://40.124.54.95:8182/graphs/walmart/tp/batch/vertices?values=[" +allInv(0)+ "," + allInv(1) + "," + allInv(2)+ "," + allInv(3)+ "," + allInv(4)+ "," +
          //        secondInv(5)+ "," + secondInv(6)+ "," + allInv(7)+"]").get().map{
          //
          //        }
          //
          val ness = Future.sequence(gettingdata)

          val combinedFuture = Future.sequence(Seq(ness.zip(putintuple)))
          val combined = combinedFuture.map(t => {
            t.flatMap(v => {
              val one = v._1
              val two = v._2
              val three = one.zip(two)
              three
            })
          })

          combined
        }
        }

        val htmlInput = for {
          fut2 <- now
        //res1 <- fut1._2
        } yield (fut2)

        val output1 = htmlInput.map {
          case result: (Seq[(Seq[childobj], newentity)]) =>
            println("result check  " + result)

            val rel = result.map(k => {
              val x = k._2
              val ch = k._1

              Map("children" -> k._1, "name" -> x.name.replaceAll("^\"|\"$", ""), "VFrequency" -> x.vFrequency.replaceAll("^\"|\"$", ""), "strength" -> x.strength.replaceAll("^\"|\"$", ""), "EFrequency" -> x.frequency.replaceAll("^\"|\"$", ""))
            })

            //        val rel = result.map(x => {
            //          Map("name" -> x.name.replaceAll("^\"|\"$", ""), "VFrequency" -> x.vFrequency.replaceAll("^\"|\"$", ""), "strength" -> x.strength.replaceAll("^\"|\"$", ""), "EFrequency" -> x.frequency.replaceAll("^\"|\"$", ""))
            //          rel
            //        }).map(x => {
            //
            //          accumulate += "data" -> Json.toJson(x)
            //        });
            //accumulate += "itr" -> itr.asInstanceOf[JsValue]

            // val objMap = Map("name" -> relation.name, "strength" -> relation.strength)
            // val json = Json.toJson(objMap)

            //Ok(views.html.rohith("Showing data in table",rel,item,fFilter))

          //Json.toJson(rel)
            rel.toString()
        }
        output1
      }
    }

    future.map(
    t => Ok(t)
    )
  }

  def addEdge() = Action(BodyParsers.parse.json){implicit request =>
    val t = request.body
    val start = (t \ "initial").get.toString()
    val end = (t \ "ending").get.toString()

    val id1 = WS.url("http://40.124.54.95:8182/graphs/walmart/vertices?key=name&value=" + start).get().map(x =>{
      val y = (x.json \ "results").as[JsArray].value
      val z = (y.head \ "_id").get.toString()
      z
    })

    val id2 = WS.url("http://40.124.54.95:8182/graphs/walmart/vertices?key=name&value=" + start).get().map(x =>{
      val y = (x.json \ "results").as[JsArray].value
      val z = (y.head \ "_id").get.toString()
      z
    })
    //WS.url("http://40.124.54.95:8182/graphs/walmart/edges?_outV="+id1+"&_label=dummy&"+ "_inV="+id2 + "&_id="+1234).post()
    Ok("Added edge with id 1234")
  }

  def addVertex() = Action(BodyParsers.parse.json){implicit request =>
    val t = request.body
    val getname = (t \ "Vname").get.toString()

    val id = 987
    val data = Json.obj(
      "uid" -> id,
      "name" -> "abcd"
     )

    //WS.url("http://40.124.54.95:8182/graphs/walmart/vertices/"+ id + "?name="+ getname)//.post()
    WS.url("http://40.124.54.95:8182/graphs/walmart/vertices/"+ id).post(data).map(s=>{
      println(s.body)
    })
    Ok("Added vertex with id 987")
  }

// ---------------------------------sarthak functions---------------------------------




}
