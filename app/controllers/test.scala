package controllers

/**
 * Created by deepanshu on 13/10/15.
 */


import play.api.Play.current
import play.api.libs.ws._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object test {

  def collect() {
    val futureResult: Future[String] = WS.url("http://107.167.178.97:8282/graphs/titanexample/vertices?key=name&value=sachin").get().map {
      response =>
        (response.json \ "results" \\ "_id").toString()
    }
    //println(futureResult)
  }

//  def main(args: Array[String]) {
//    test.collect()
//  }
}

