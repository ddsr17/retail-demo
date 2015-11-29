package controllers

import java.io.{BufferedOutputStream, FileOutputStream}
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.lang3.ObjectUtils.Null
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
import scala.util.{Try, Failure, Success}
import scala.concurrent._
import ExecutionContext.Implicits.global

case class entityobj(name:String,vFrequency: String,strength:String, frequency: String)
case class childobj(name: String,strength: String,frequency: String)
case class newentity(id: String,name:String,vFrequency: String,strength:String, frequency: String)

class Application extends Controller {

  def options(path:String) = Action { Ok("")}

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def display3 = Action {
    Ok(views.html.sarthak())
  }

  var allInv : Seq[String] = Seq()
  var secondInv: Seq[String] = Seq()
  val nResults = 10

  def getVertices3 = Action.async(BodyParsers.parse.json) { implicit request =>
    val tt = request.body

    val item = (tt \ "news").get.toString().replaceAll("^\"|\"$", "")
    val fFilter = (tt \ "frequencyfilter").get.toString().replaceAll("^\"|\"$", "").toDouble

    val newsItem = item.toLowerCase()
    var freq = ""
    println(newsItem)


    val futureResult = WS.url("http://40.124.54.95:8182/graphs/capillary/vertices?key=name&value=" + item).withHeaders("Accept" -> "application/json").get().map {
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

        val id = (temp.head \ "_id").get.toString()

        (id, freq)
      }
    }


    var thirdInv: Seq[Seq[String]] = Seq()

    val future = futureResult.flatMap{a =>{

      var data: Seq[Seq[JsValue]] = Seq()

      val output = WS.url("http://40.124.54.95:8182/graphs/capillary/vertices/" + a._1 + "/outE?_label=relation&_take=" + nResults).withHeaders("Accept" -> "application/json").get().map {
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


          val afilter = check.filter(x => {
            x._2._1 > fFilter
          })

          afilter
        })

      val now = response2.flatMap { x => {
        val qString = allInv.mkString(",")
        val getallnames = WS.url("http://40.124.54.95:8182/graphs/capillary/tp/batch/vertices?values=[" + qString + "]").get().map {
          result => {
            val tmp = (result.json \ "results").as[JsArray].value
            val gettuple = tmp.map(x => {
              val name = (x \ "name").get.toString()
              val regex = name.substring(1,name.indexOf("V")-2)
              val frequency = (x \ "frequency").get.toString()
              val tuple = (regex, frequency)
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

        println("got 1st degree nodes")
        val secondLevelEdges = x.map(d => {
          var seq: Seq[String] = Seq()
          val parentid = d._1

          val child: Future[Seq[JsValue]] = WS.url("http://40.124.54.95:8182/graphs/capillary/vertices/" + parentid + "/outE?_label=relation&_take=" + nResults).get().map(res => {
            (res.json \ "results").as[JsArray].value
          })

          val childata = child.map(valueSeq => {

            val changetoobj = valueSeq.map(jsValue => {
              val str = (jsValue \ "strength").get.toString()
              val frq = (jsValue \ "frequency").get.toString()
              val inid = (jsValue \ "_inV").get.toString()
              secondInv = secondInv :+ inid

              childobj(inid, str, frq)
            })

            val y = changetoobj.take(3)
            y
          })

          childata
        })

        println("got second degree nodes")
        println("now combining first and second degree nodes")
        val ness = Future.sequence(secondLevelEdges)

        val changedtoname = ness.flatMap(x => {
          val seq = secondInv.mkString(",")

          val allnames = WS.url("http://40.124.54.95:8182/graphs/capillary/tp/batch/vertices?values=[" + seq +"]").get().map{result =>{
            val allvalues = (result.json \ "results").as[JsArray].value
            val gg = allvalues.map(onevalue =>{
              val name=  (onevalue \ "name").get.toString()
              val regex = name.substring(1,name.indexOf("V")-2)
              val id = (onevalue \ "_id").get.toString()
              (id,regex)
            })
            gg
          }
          }

          var m:Map[String,String] = Map()

          val all =allnames.map(d => {
            d.map{r =>{
              m += r._1 -> r._2
              r
            }}
            d
          })

          val withnames = x.map(y => {
            val ii = y.map(z =>{
              val kk = all.map(t => {
                var a = m.get(z.name).get
                childobj(a,z.strength,z.frequency)
              })
              kk
            })
            Future.sequence(ii)
          })
          Future.sequence(withnames)
        })


        val combinedFuture = Future.sequence(Seq(changedtoname.zip(putintuple)))
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
        res1 <- futureResult
      } yield (res1._2,fut2)

      val output1 = htmlInput.map {
        case result: (String,Seq[(Seq[childobj], newentity)]) =>
          println("sending result")
          val rel = result._2.map(k => {
            val x = k._2
            val ch = k._1

            val tojson = Json.toJson(ch.map(y => {
              Map("name" -> y.name, "strength" -> y.strength, "frequency" -> y.frequency)
            }))


            Map("children" -> tojson.toString(), "name" -> x.name.replaceAll("^\"|\"$", ""), "VFrequency" -> x.vFrequency.replaceAll("^\"|\"$", ""), "strength" -> x.strength.replaceAll("^\"|\"$", ""), "EFrequency" -> x.frequency.replaceAll("^\"|\"$", ""), "entityid" -> result._1)
          })

          Json.toJson(rel)
      }
      output1
    }
    }

    future.map(
      t => Ok(t.toString())
    )
  }

  def addEdge() = Action(BodyParsers.parse.json){implicit request =>
    val t = request.body
    val start = (t \ "initial").get.toString().replaceAll("^\"|\"$", "")
    val end = (t \ "ending").get.toString().replaceAll("^\"|\"$", "")
    println(start)
    println(end)

    val id1 = WS.url("http://40.124.54.95:8182/graphs/capillary/vertices?key=name&value=" + start).get().map(x =>{
      val y = (x.json \ "results").as[JsArray].value
      val z = (y.head \ "_id").get.toString()
      println("id1 is " + z)
      z
    })

    val id2 = WS.url("http://40.124.54.95:8182/graphs/capillary/vertices?key=name&value=" + end).get().map(x =>{
      val y = (x.json \ "results").as[JsArray].value
      val z = (y.head \ "_id").get.toString()
      println("id2 is " + z)
      z
    })

    val tt = id1.map(x => {
      id2.map(y => {
        val data = Json.obj(
          "_outV" -> x,
          "_inV"  -> y,
          "_label" -> "dummy"
        )
        println("creating edge")
        WS.url("http://40.124.54.95:8182/graphs/capillary/edges").post(data).map(s => {
          println(s.body)
        })
        y
      })
      x
    })
    Ok("Added edge with id 1234")
  }

  def addVertex() = Action(BodyParsers.parse.json){implicit request =>
    val t = request.body
    val getname = (t \ "Vname").get.toString().replaceAll("^\"|\"$", "")

    val id = 987
    val data = Json.obj(
      "uid" -> id,
      "name" -> getname
    )

    WS.url("http://40.124.54.95:8182/graphs/capillary/vertices/").post(data).map(s=>{
      println(s.body)
    })
    Ok("Added vertex with id 987")
  }

}
