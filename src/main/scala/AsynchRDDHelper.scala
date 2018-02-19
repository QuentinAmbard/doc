import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import AsynchRDDHelper.AsynchConfig
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Iterator
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

/**
  * Asynch Iterator
  */
class AsynchPartitionIterator[A](iterator: Iterator[A])(implicit ec: ExecutionContextExecutor) extends Iterator[A] {
  def mapAsync[B](f: A => B): Iterator[Future[B]] = new Iterator[Future[B]] {
    def hasNext = iterator.hasNext

    def next() = {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  def foreachAsync[U](f: A => U) {
    while (hasNext) {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): A = iterator.next()
}


/**
  * Thread pool holder, to share a specific thread pool.
  */
object ThreadPoolHolder {
  val lock = new Object()
  var pool: Option[ExecutionContextExecutor] = None

  def getInstance(poolSize: Int) = {
    lock.synchronized {
      if (pool.isEmpty) {
        val executorWaiting = new ThreadPoolExecutor(poolSize, poolSize + 1, 1, TimeUnit.MINUTES, new ArrayBlockingQueue[Runnable](poolSize * 2))
        pool = Some(ExecutionContext.fromExecutor(executorWaiting))
      }
      pool.get
    }
  }
}

/**
  * Batch rows and execute them in an asynch fashion.
  */
object AsynchRDDProcessor {

  def processBatch[RESULT, ROW](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit), partition: Iterator[ROW], asynchConfig: AsynchConfig) = {
    val threadPool = ThreadPoolHolder.getInstance(asynchConfig.poolSize)
    val asynchIt: AsynchPartitionIterator[ROW] = new AsynchPartitionIterator[ROW](partition)(threadPool)
    val (futureIterator, close) = f(asynchIt)
    // Perform the sliding technique to queue & process only a fixed amount of records at a time
    val slidingIterator = futureIterator.sliding(asynchConfig.batchSize - 1)
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map(futureBatch => {
      Await.result(futureBatch.head, asynchConfig.timeout seconds)
    }) ++
      tailIterator.flatMap(lastBatch => {
        val r = lastBatch.map(future => Await.result(future, asynchConfig.timeout seconds))
        close()
        r
      })
  }
}

/**
  * Implicit helper.
  * 3 parameters can be tuned:
  * - the pool size (how many thread are created),
  * - the batch size (how many tasks are executed simultaneously)
  * - the future timeout
  */
object AsynchRDDHelper {
  val CONF_POOL_SIZE = "spark.multithreading.pool.size"
  val CONF_BATCH_SIZE = "spark.multithreading.batch.size"
  val CONF_TIMEOUT = "spark.multithreading.timeout.sec"
  private val logger: Logger = LoggerFactory.getLogger(AsynchRDDHelper.getClass)

  case class AsynchConfig(poolSize: Int, batchSize: Int, timeout: Int)

  implicit class AsyncElementPartitionRDD[ROW: ClassTag](rdd: RDD[ROW]) {

    def mapPartitionAsynchWithSession[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW], Session) => Iterator[Future[RESULT]]) = {
      val conf = rdd.sparkContext.getConf
      val asynchConfig = buildAsynchConfig(conf)
      val cassandraConnector = CassandraConnector(conf)
      rdd.mapPartitions(partition => {
        val session = cassandraConnector.openSession()
        AsynchRDDProcessor.processBatch((it: AsynchPartitionIterator[ROW]) => {
          (f(it, session), () => session.close())
        }, partition, asynchConfig)
      })
    }

    def mapPartitionAsynch[RESULT: ClassTag](f: AsynchPartitionIterator[ROW] => Iterator[Future[RESULT]]) = {
      mapPartitionAsynchClosable((it: AsynchPartitionIterator[ROW]) => {
        (f(it), () => Unit)
      })
    }

    def mapPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      val asynchConfig = buildAsynchConfig(rdd.sparkContext.getConf)
      rdd.mapPartitions(partition => {
        AsynchRDDProcessor.processBatch(f, partition, asynchConfig)
      })
    }

    def foreachPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      val asynchConfig = buildAsynchConfig(rdd.sparkContext.getConf)
      rdd.foreachPartition(partition => {
        AsynchRDDProcessor.processBatch(f, partition, asynchConfig)
      })
    }
  }

  private def buildAsynchConfig(conf: SparkConf) = {
    AsynchConfig(getConfWithDefault(conf, CONF_POOL_SIZE), getConfWithDefault(conf, CONF_BATCH_SIZE), getConfWithDefault(conf, CONF_TIMEOUT))
  }

  private def getConfWithDefault(conf: SparkConf, value: String, default: Int = 10): Int = {
    if (conf.contains(value)) {
      conf.get(value).toInt
    }
    else {
      logger.info(s"spark conf $value isn't set, use $default as default")
      default
    }
  }
}
