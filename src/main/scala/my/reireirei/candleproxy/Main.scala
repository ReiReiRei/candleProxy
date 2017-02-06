package my.reireirei.candleproxy

import javafx.concurrent.Worker

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, RoundRobinRoutingLogic, Router}
import com.typesafe.config.ConfigFactory
import my.reireirei.candleproxy.CandleStorage.TicCandles
import my.reireirei.candleproxy.Observer.{MinuteClosed, NewClient}

/**
  * Created by ReiReiRei on 2/6/2017.
  */
object TickerClientServer extends App {

  val config = ConfigFactory.parseString("akka.loglevel = DEBUG")
  implicit val system = ActorSystem("candleProxy", config)

  run()

  def run(): Unit = {
    system.actorOf(Observer.props())

  }

}

object Observer {

  sealed trait ObserverCmd

  case class NewClient(handler: ActorRef)

  case class MinuteClosed(candles: TicCandles)
  def props(): Props = Props(classOf[Observer])

}

class Observer extends Actor {
  val candleStorage: ActorRef = context.actorOf(CandleStorage.props(self))
  val tickersStorage: ActorRef = context.actorOf(TickersStorage.props(candleStorage))

  val client =context.actorOf(Client.props())
  val server = context.actorOf(Server.props())

  context watch candleStorage
  context watch tickersStorage

  var router: Router = {
    Router(BroadcastRoutingLogic())
  }

  override def receive: Receive = {
    case NewClient(handler) =>
      router.addRoutee(handler)
      candleStorage ! CandleStorage.GetHistoryCandles
    case MinuteClosed(candles) =>
      router.route(ClientHandler.MinuteClosed(candles), sender)
    case Terminated(a) =>
      router = router.removeRoutee(a)

  }
}

