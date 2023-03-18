package project.controller

import project.server.AppServer

class AppController {

  val server: AppServer = new AppServer

  def dispatch(): Unit = {
    println(server.analysis.mkString(","))
  }

}
