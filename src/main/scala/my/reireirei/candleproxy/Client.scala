package my.reireirei.candleproxy

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props}
import akka.io._
import akka.util.{ByteString, CompactByteString}

import scala.annotation.tailrec


/**
  * Created by ReiReiRei on 2/5/2017.
  */
object Frames {

  def getTickersFromRaw(data: ByteString): (List[Ticker], ByteString) = {

    val headerSize = 2

    @tailrec
    def multiPacket(packets: List[Ticker], current: ByteString): (List[Ticker], ByteString) = {
      if (current.length < headerSize) {
        (packets.reverse, current)
      } else {
        val len = current.iterator.getShort
        if (len < 0) throw new RuntimeException(s"Invalid packet length: $len")
        if (current.length < len + headerSize) {
          (packets.reverse, current)
        } else {
          val (front, back) = current.splitAt(len + headerSize)
          val ticker = Ticker.fromPacket(front)
          multiPacket(ticker :: packets, back)
        }
      }

    }

    multiPacket(List[Ticker](), data)
  }
}
object Client {
  def props(remote: InetSocketAddress, listener: ActorRef,candleStorage: ActorRef,tickersStorage: ActorRef) =
    Props(classOf[Client],remote,listener,candleStorage,tickersStorage)
}
class Client(remote: InetSocketAddress, listener: ActorRef,candleStorage: ActorRef,tickersStorage: ActorRef) extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def updateTickers(connection: ActorRef, data: ByteString): Receive = {
    case CommandFailed(w: Write) =>
      listener ! "Write failed"
    case Received =>
      val (tickers, tail) = Frames.getTickersFromRaw(data)
      tickersStorage ! tickers
      context become updateTickers(connection, tail)
    case "lose" =>
      connection ! Close
    case _: ConnectionClosed =>
      listener ! "connection closed"
      context stop self
  }

  def receive: Receive = {
    case CommandFailed(_: Connect) =>
      listener ! "Connect failed"
      context stop self

    case c@Connected(remote, local) =>
      listener ! c
      val connection = sender()
      connection ! Register(self)
      context become updateTickers(connection, CompactByteString())
  }
}
