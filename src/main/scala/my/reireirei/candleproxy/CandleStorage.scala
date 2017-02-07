package my.reireirei.candleproxy

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import my.reireirei.candleproxy.CandleStorage.{AddCandles, GetHistoryCandles, TicCandles}
import my.reireirei.candleproxy.ClientHandler.DeliverHistoryToNewClient
import my.reireirei.candleproxy.Observer.MinuteClosed

import scala.collection.mutable

object CandleStorage {
  type TicCandles = Map[String, Candle]

  sealed trait CandleStorageCmd

  case class GetHistoryCandles(handler: ActorRef) extends CandleStorageCmd

  case class AddCandles(candles: TicCandles) extends CandleStorageCmd

  def props(candlesWatcher: ActorRef): Props = Props(classOf[CandleStorage], candlesWatcher)

}

class CandleStorage(candlesWatcher: ActorRef) extends Actor with ActorLogging {
  val queueSize = 5
  val storage: mutable.Queue[TicCandles] = mutable.Queue[TicCandles]()

  override def receive: Receive = {
    case AddCandles(candles) => storage += candles
      if (storage.size > queueSize) storage.dequeue()
      candlesWatcher ! MinuteClosed(candles)
    case GetHistoryCandles(handler) =>
      val history = storage.toList
      handler ! DeliverHistoryToNewClient(history)
  }
}
