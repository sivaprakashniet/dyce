package rediscache

import javax.inject.Inject
import com.redis._
import play.api.Configuration

class Cache @Inject()(configuration: Configuration) {

  val host = configuration.underlying.getString("redis.host")
  val port = configuration.underlying.getString("redis.port")

  val r = new RedisClient(host, port.toInt)

  def set(key: String, value: String): Boolean = r.set(key, value)

  def get(key: String): Option[String] = r.get(key)
}
