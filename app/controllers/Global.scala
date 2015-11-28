package controllers

import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import play.api.GlobalSettings

import scala.concurrent.Future

/**
 * Created by deepanshu on 19/10/15.
 */
object Global extends GlobalSettings{

  override def onError(request: RequestHeader, ex: Throwable) = {
    Future.successful(InternalServerError(
      views.html.errorPage(ex.getMessage)
    ))
  }

}
