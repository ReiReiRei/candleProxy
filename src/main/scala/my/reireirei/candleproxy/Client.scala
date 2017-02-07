package my.reireirei.candleproxy

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io._
import akka.util.{ByteString, CompactByteString}
import my.reireirei.candleproxy.TickersStorage.Add
import org.log4s._

import scala.annotation.tailrec


/**
  * Created by ReiReiRei on 2/5/2017.
  */

object Client {
  def props(remote: InetSocketAddress, listener: ActorRef, candleStorage: ActorRef, tickersStorage: ActorRef) =
    Props(classOf[Client], remote, listener, candleStorage, tickersStorage)
}

class Client(remote: InetSocketAddress, listener: ActorRef, candleStorage: ActorRef, tickersStorage: ActorRef) extends Actor with ActorLogging {

  import Tcp._
  import context.system


  IO(Tcp) ! Connect(remote)

  def updateTickers(connection: ActorRef, buf: ByteString):Receive = {
        case CommandFailed(w: Write) =>
      listener ! "Write failed"
    case Received(data) =>
      val (tickers, tail) = getTickersFromRaw(buf ++ data)
      system.log.debug("new tickers:" + tickers.toString())
      tickersStorage ! Add(tickers)
      context become updateTickers(connection, tail)
    case "lose" =>
      connection ! Close
    case _: ConnectionClosed =>
      listener ! "connection closed"
      context stop self
  }

  def receive = {
    case CommandFailed(_: Connect) =>
      listener ! "Connect failed"
      context stop self

    case Connected(remote, local) =>
      listener ! Connected(remote, local)
      val connection = sender()
      connection ! Register(self)
      system.log.debug("become update tickers")
      context become updateTickers(connection, CompactByteString())
  }

  def getTickersFromRaw(data: ByteString): (List[Ticker], ByteString) = {

    val headerSize = 2
    implicit val byteOrder = ByteOrder.BIG_ENDIAN

    val log = getLogger
    log.debug("Data: " + data.toString())

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
