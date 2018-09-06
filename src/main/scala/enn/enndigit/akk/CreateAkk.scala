//package enn.enndigit.akk
//
//import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import akka.event.{Logging, LoggingAdapter}
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/7
//  * @Project:sparktest
//  * @Package:enn.enndigit.akk
//  */
//object CreateAkk extends App{
//
//  class CreateAkk extends Actor {
//    val log = Logging(context.system, this)
//
//    def receive = {
//      case "test" => log.info("received test")
//      case _      => log.info("received unknown message")
//    }
//  }
//  //创建ActorSystem对象
//  val system = ActorSystem("MyActorSystem")
//  //返回ActorSystem的LoggingAdpater
//  val systemLog=system.log
//  //创建MyActor,指定actor名称为myactor
//  val myactor = system.actorOf(Props[CreateAkk], name = "myactor")
//
//  systemLog.info("准备向myactor发送消息")
//  //向myactor发送消息
//  myactor!"test"
//  myactor! 123
//
//  //关闭ActorSystem，停止程序的运行
//  system.shutdown()
//}
