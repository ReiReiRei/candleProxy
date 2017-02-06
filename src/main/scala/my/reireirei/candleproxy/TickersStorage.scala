package my.reireirei.candleproxy

import akka.actor.{Actor, ActorRef, Props}
import my.reireirei.candleproxy.CandleStorage.{AddCandles, GetHistoryCandles, TicCandles}
import my.reireirei.candleproxy.ClientHandler.DeliverHistoryToNewClient

import scala.collection.mutable


/**
  * Created by ReiReiRei on 2/5/2017.
  */
object TickersStorage {

  sealed trait TickerCmd

  case class Add(ticker: List[Ticker]) extends TickerCmd

  case class ForceUpdateCandles(min: Long) extends TickerCmd

  def props(candleStorage: ActorRef): Props = Props(classOf[TickersStorage], candleStorage)
}

class TickersStorage(candleStorage: ActorRef) extends Actor {

  import scala.collection.mutable
  import TickersStorage._

  var currentMinuteSet = false
  var currentMinute: Long = 0L
  var alreadySent = false


  type TickerMap = mutable.HashMap[String, List[Ticker]]

  val storage = new TickerMap()

  context watch candleStorage

  override def receive: Unit = {
    case Add(tickers) => tickers.foreach { ticker =>
      if (currentMinuteSet && currentMinute == ticker.minute) {
        storage(ticker.ticker) = storage.get(ticker.ticker) match {
          case None => List(ticker)
          case Some(xs) => ticker :: xs
        }
      } else if (currentMinute < ticker.minute) {
        val candles = storage.toMap.mapValues(Candle.fromTickers(_))
        candleStorage ! AddCandles(candles)
        storage.clear()
        storage(ticker.ticker) = List(ticker)
        currentMinute = ticker.minute
        currentMinuteSet = true
      } else {
      }
    }
    case ForceUpdateCandles(minute) =>
      val candles = storage.toMap.mapValues(Candle.fromTickers(_))
      candleStorage ! AddCandles(candles)
      storage.clear()
      currentMinute = minute
      currentMinuteSet = true
  }


}

object CandleStorage {
  type TicCandles = Map[String, Candle]

  sealed trait CandleStorageCmd

  case class GetHistoryCandles(handler:ActorRef) extends CandleStorageCmd

  case class AddCandles(candles: TicCandles) extends CandleStorageCmd

  def props(candlesWatcher: ActorRef): Props = Props(classOf[CandleStorage], candlesWatcher)

}

class CandleStorage(candlesWatcher: ActorRef) extends Actor {
  val queueSize = 5
  val storage: mutable.Queue[TicCandles] = mutable.Queue[TicCandles]()

  override def receive: Receive = {
    case AddCandles(candles) => storage += candles
      if (storage.size > queueSize) storage.dequeue()
      candlesWatcher ! candles
    case GetHistoryCandles(handler) =>
      val history = storage.toList
      handler ! DeliverHistoryToNewClient(history)
  }
}
