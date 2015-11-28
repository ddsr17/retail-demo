package controllers

import play.api.mvc.{Result, RequestHeader}

import scala.concurrent.Future

/**
 * Created by deepanshu on 19/10/15.
 */
object HttpErrorHandler {

  def onClientError(request: RequestHeader,statusCode: Int,message: String): Future[Result] = {

    views.html.index
    return null
  }

  def onServerError(request: RequestHeader,exception: Throwable): Future[Result] = {

    views.html.index
    return null
  }

}
