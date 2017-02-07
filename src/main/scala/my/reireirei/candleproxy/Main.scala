package my.reireirei.candleproxy

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.routing.{BroadcastRoutingLogic, Router}
import com.typesafe.config.{Config, ConfigFactory}
import my.reireirei.candleproxy.CandleStorage.TicCandles
import my.reireirei.candleproxy.Observer.{MinuteClosed, NewClient}

/**
  * Created by ReiReiRei on 2/6/2017.
  */
object TickerClientServer extends App {

  val config = ConfigFactory.load()
  implicit val system = ActorSystem("candleProxy", config)

  try run()
  catch {
    case _:Throwable => system.terminate()
  }

  def run(): Unit = {
    system.actorOf(Observer.props(config))

  }

}

object Observer {

  sealed trait ObserverCmd

  case class NewClient(handler: ActorRef)

  case class MinuteClosed(candles: TicCandles)

  def props(config: Config): Props = Props(classOf[Observer], config)
}

class Observer(config: Config) extends Actor with ActorLogging{
  val candleStorage: ActorRef = context.actorOf(CandleStorage.props(self),"CandleStorage")
  val tickersStorage: ActorRef = context.actorOf(TickersStorage.props(candleStorage),"TickerStorage")
  val local = new InetSocketAddress("localhost", 5556)
  val remote = new InetSocketAddress("localhost", 5555)


  private val client = context.actorOf(Client.props(remote, self, candleStorage, tickersStorage),"TickersClient")
  private val server = context.actorOf(Server.props(local, self),"Server")

  context watch candleStorage
  context watch tickersStorage

  var router: Router = {
    Router(BroadcastRoutingLogic())
  }



  override def receive: Receive = {
    case NewClient(handler) =>
      router = router.addRoutee(handler)
      candleStorage ! CandleStorage.GetHistoryCandles(handler)
    case MinuteClosed(candles) =>
      router.route(ClientHandler.MinuteClosed(candles), sender)
    case Terminated(a) =>
      router = router.removeRoutee(a)

  }
}

