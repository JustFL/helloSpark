package project

import project.common.Environment
import project.controller.AppController

object Application extends App with Environment{

  createEnvironment(){
    val controller: AppController = new AppController
    controller.dispatch()
  }

}
