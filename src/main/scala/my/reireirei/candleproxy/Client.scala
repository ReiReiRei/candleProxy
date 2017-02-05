package my.reireirei.candleproxy

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.io._
import akka.util.ByteString

import scala.annotation.tailrec


/**
  * Created by ReiReiRei on 2/5/2017.
  */
object Buffering {

  def getPacket(data: ByteString): (List[Ticker], ByteString) = {

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
          val (front, back) = current.splitAt(len+headerSize)
          val ticker = Ticker.fromPacket(front)
          multiPacket(ticker :: packets, back)
        }
      }
    }
    multiPacket(List[Ticker](), data)
  }
}

class Client(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      listener ! "Connect failed"
      context stop self

    case c@Connected(remote, local) =>
      listener ! c
      val connection = sender()
      connection ! Register(self)
      context become {
        case CommandFailed(w: Write) =>
             listener ! "write failed"
        case Received(data) =>
          listener ! data
        case "lose" =>
          connection ! Close
        case _: ConnectionClosed =>
          listener ! "connection closed"
          context stop self
      }
  }


}
