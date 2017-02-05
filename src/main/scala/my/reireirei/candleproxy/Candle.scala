package my.reireirei.candleproxy

import java.time.LocalDateTime

import akka.util.ByteString

/**
  * Created by ReiReiRei on 2/5/2017.
  */
/*
Наш сервер соединяется к провайдеру данных по протоколу TCP/IP, после чего тот начинает присылать биржевые сделки в виде сообщений следующего формата:

[ LEN:2 ] [ TIMESTAMP:8 ] [ TICKER_LEN:2 ] [ TICKER:TICKER_LEN ] [ PRICE:8 ] [ SIZE:4 ]

где поля имеют следующую семантику:

LEN: длина последующего сообщения (целое, 2 байта)
TIMESTAMP: дата и время события (целое, 8 байт, milliseconds since epoch)
TICKER_LEN: длина биржевого тикера (целое, 2 байта)
TICKER: биржевой тикер (ASCII, TICKER_LEN байт)
PRICE: цена сделки (double, 8 байт)
SIZE: объем сделки (целое, 4 байта)
*/
case class Ticker(timeStamp: Long, ticker: String, price: Double, size: Int)

object Ticker {
  def fromPacket(datum: ByteString): Ticker = {
    val iter = datum.iterator
    val length = iter.getShort
    val timestamp = iter.getLong
    val tickerLength = iter.getShort
    val ticker = new String(iter.getBytes(tickerLength).map(_.toChar))
    val price = iter.getDouble
    val size = iter.getInt
    Ticker(timestamp, ticker, price, size)
  }
}

//{ "ticker": "AAPL", "timestamp": "2016-01-01T15:02:00Z", "open": 112.1, "high": 115.2, "low": 110.0, "close": 114.2, "volume": 13000 }
case class Candle(ticker: String, timestamp: LocalDateTime, open: Double, high: Double, low: Double, close: Double, volume: Long)

object Candle {
  def fromTickers(tickers: Seq[Ticker]): Candle = {
    val ticker = tickers(0).ticker
    val timestamp = new LocalDateTime(roundToNextMinute(tickers(0).timeStamp))
    val open = tickers.minBy(_.timeStamp).price
    val close = tickers.maxBy(_.timeStamp).price
    val low = tickers.minBy(_.timeStamp).price
    val high = tickers.maxBy(_.price).price
    val volume = tickers.map(_.size).sum
    Candle(ticker, timestamp, open, high, low, close, volume)
  }

  def roundToNextMinute(mills: Long): Long = {
    mills - (mills % 60 * 1000) + 60 * 1000
  }
}
