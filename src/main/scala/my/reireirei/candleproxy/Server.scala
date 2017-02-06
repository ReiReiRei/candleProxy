package my.reireirei.candleproxy

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props, SupervisorStrategy}
import akka.io.{IO, Tcp}
import akka.io.Tcp.{Event, PeerClosed, Write}
import akka.util.ByteString
import my.reireirei.candleproxy.CandleStorage.TicCandles
import my.reireirei.candleproxy.ClientHandler.{DeliverHistoryToNewClient, MinuteClosed, Send}

/**
  * Created by ReiReiRei on 2/5/2017.
  */

object Server {
  def props(remote: InetSocketAddress,observer: ActorRef) = Props(classOf[Server],remote,observer)
}
class Server(remote: InetSocketAddress,observer: ActorRef) extends Actor {

  import Tcp._
  import context.system

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    IO(Tcp) ! Bind(self, remote)
  }

  // do not restart
  override def postRestart(thr: Throwable): Unit = context stop self

  def receive: Receive = {
    case Bound(localAddress) =>

    case CommandFailed(Bind(_, local, _, _, _)) =>
      context stop self

    case Connected(remote, local) =>
      val handler = context.actorOf(ClientHandler.props(sender, remote))
      sender ! Register(handler, keepOpenOnPeerClosed = true)
      observer ! Observer.NewClient(handler)

  }

}

object ClientHandler {

  sealed trait ClientHandlerCmd

  case class DeliverHistoryToNewClient(history: List[TicCandles]) extends ClientHandlerCmd

  case class MinuteClosed(candles: TicCandles)

  case class Send(data: ByteString)

  def props(connection: ActorRef, remote: InetSocketAddress) = Props(classOf[ClientHandler], connection, remote)

}

class ClientHandler(connection: ActorRef, remote: InetSocketAddress) extends Actor {

  var storage = Vector.empty[ByteString]
  var stored = 0L
  var transferred = 0L
  var closing = false

  case object Ack extends Event


  context watch connection

  override def receive = {

    case MinuteClosed(candles) =>
      candles.foreach { case (k, v) => self ! Send(v.toByteString) }
    case DeliverHistoryToNewClient(history) =>
      history.foreach { candles =>
        candles.foreach { case (k, v) => self ! Send(v.toByteString) }
      }

    case Send(data) =>
      buffer(data)
      connection ! Write(data, Ack)

      context.become({
        case Send(data) => buffer(data)
        case MinuteClosed(candles) => candles.foreach { case (k, v) => self ! buffer(v.toByteString) }
        case DeliverHistoryToNewClient(history) =>
          history.foreach { candles =>
            candles.foreach { case (k, v) => self ! buffer(v.toByteString) }
          }
        case Ack => acknowledge()
        case PeerClosed => closing = true
      }, discardOld = false)

    case PeerClosed => context stop self
  }

  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size
  }

  private def acknowledge(): Unit = {
    require(storage.nonEmpty, "storage was empty")
    val size = storage(0).size
    stored -= size
    transferred += size
    storage = storage drop 1
    if (storage.isEmpty) {
      if (closing) context stop self
      else context.unbecome()
    } else connection ! Write(storage(0), Ack)
  }
}

