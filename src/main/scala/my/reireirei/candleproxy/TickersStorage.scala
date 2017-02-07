package my.reireirei.candleproxy

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import my.reireirei.candleproxy.CandleStorage.AddCandles

import scala.concurrent.duration._


/**
  * Created by ReiReiRei on 2/5/2017.
  */
object TickersStorage {

  sealed trait TickerCmd

  case class Add(ticker: List[Ticker]) extends TickerCmd

  case class ForceUpdateCandles(min: Long) extends TickerCmd

  def props(candleStorage: ActorRef): Props = Props(classOf[TickersStorage], candleStorage)
}

class TickersStorage(candleStorage: ActorRef) extends Actor with ActorLogging {

  import TickersStorage._
  import context.dispatcher

  import scala.collection.mutable

  var currentMinuteSet = false
  var currentMinute: Long = 0L

  type TickerMap = mutable.HashMap[String, List[Ticker]]

  val storage = new TickerMap()

  context watch candleStorage

  def receive: Receive = {
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

        val wait = 60*1000 - System.currentTimeMillis() % (60*1000)

        context.system.scheduler.scheduleOnce(wait millisecond, self, ForceUpdateCandles(currentMinute+1))

      } else {
      }
    }
    case ForceUpdateCandles(minute) if minute >  currentMinute =>
      val candles = storage.toMap.mapValues(Candle.fromTickers(_))
      candleStorage ! AddCandles(candles)
      storage.clear()
      currentMinute = minute
      currentMinuteSet = true

      val wait = 60*1000 - System.currentTimeMillis() % (60*1000)


      context.system.scheduler.scheduleOnce(wait millisecond, self, ForceUpdateCandles(currentMinute+1))
  }
}
