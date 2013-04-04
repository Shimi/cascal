package com.shorrockin.cascal.session

import scala.language.implicitConversions
import scala.collection.mutable
import collection.immutable.HashSet
import java.util.concurrent.atomic.AtomicLong
import java.util.{Map => JMap, List => JList, HashMap, ArrayList}
import java.nio.ByteBuffer
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.cassandra.thrift.{Mutation, Cassandra, NotFoundException, ConsistencyLevel}
import org.apache.cassandra.thrift.{Column => CassColumn}
import org.apache.cassandra.thrift.{SuperColumn => CassSuperColumn}
import com.shorrockin.cascal.model._
import com.shorrockin.cascal.utils.Conversions._
import com.shorrockin.cascal.utils.Utils.now
import scala.collection.mutable.LinkedHashMap
import scala.collection.Map

/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val host:Host, val defaultConsistency:Consistency, val noFramedTransport:Boolean) extends SessionTemplate {

  def this(host:String, port:Int, timeout:Int, defaultConsistency:Consistency, noFramedTransport:Boolean) = this(Host(host, port, timeout), defaultConsistency, noFramedTransport)
  def this(host:String, port:Int, timeout:Int, defaultConsistency:Consistency) = this(host, port, timeout, defaultConsistency, false)
  def this(host:String, port:Int, timeout:Int) = this(host, port, timeout, Consistency.One, false)

  private val sock = {
    if (noFramedTransport) {
      new TSocket(host.address, host.port, host.timeout)
    } else {
      new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    }
  }

  private val protocol = new TBinaryProtocol(sock)

  var lastError: Option[Throwable] = None

  val client = new Cassandra.Client(protocol)

  /**
   * opens the socket
   */
  def open() = sock.open()


  /**
   * closes this session
   */
  def close() = {
    sock.close()
    protocol.getTransport.close()
  }


  /**
   * returns true if this session is open
   */
  def isOpen = sock.isOpen


  /**
   * true if the we experienced an error while using this session
   */
  def hasError = lastError.isDefined


  /**
   * return the current cluster name of the cassandra instance
   */
  lazy val clusterName = client.describe_cluster_name()


  /**
   * returns the version of the cassandra instance
   */
  lazy val version = client.describe_version()


  /**
   * returns all the keyspaces from the cassandra instance
   */
  lazy val keyspaces: Seq[String] = Buffer(client.describe_keyspaces.map { _.name })

  /**
   * returns the descriptors for all keyspaces
   */
  lazy val keyspaceDescriptors: Set[Tuple3[String, String, String]] = {
    var keyspaceDesc: Set[Tuple3[String, String, String]] = new HashSet[Tuple3[String, String, String]]
    client.describe_keyspaces foreach {
      space =>
        val familyMap = space.cf_defs
        familyMap foreach {
          family =>
            keyspaceDesc = keyspaceDesc + ((space.name, family.name, family.column_type))
            ()
        }
    }
    keyspaceDesc
  }

  /**
   * Cassandra 0.7 requires you to set the keyspace before CRUD operations. We cache the last
   * set keyspace and change it in the client if the new operation differs.
   */
  private var currentKeyspace:String = null

  def verifyKeyspace(keyspace:String) = {
    if (keyspace == null || keyspace.length == 0)
      throw new IllegalArgumentException("Keyspace cannot be null")
    if (currentKeyspace == null || !keyspace.equals(currentKeyspace)) {
      client.set_keyspace(keyspace)
      currentKeyspace = keyspace
    }
  }

  def verifyInsert[E](col: Column[E]) {
    var famType = if (col.owner.isInstanceOf[SuperColumn]) "Super" else "Standard"
    if (!keyspaceDescriptors.contains(col.keyspace.value, col.family.value, famType)) {
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s of type %s does not exist in this cassandra instance".format(col.keyspace.value, col.family.value, famType))
    }
  }

  def verifyRemove(container: ColumnContainer[_, _]) {
    if (!keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Standard") &&
            !keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Super"))
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s does not exist in this cassandra instance".format(container.keyspace.value, container.family.value))
  }

  def verifyOperation(op: Operation) {
    if (op.isInstanceOf[Insert]) {
      verifyInsert(op.asInstanceOf[Insert].column)
    } else if (op.isInstanceOf[Delete]) {
      verifyRemove(op.asInstanceOf[Delete].container)
    }
  }

  def truncate(cfname: String) = {
    client.truncate(cfname)
  }
  
  /**
   *  returns the column value for the specified column
   */
  def get[ResultType, ValueType](col: Gettable[ResultType, ValueType], consistency: Consistency): Option[ResultType] = detect {
    verifyKeyspace(col.keyspace.value)
    try {
      verifyKeyspace(col.keyspace.value)
      val result = client.get(ByteBuffer.wrap(col.key.value.getBytes("UTF-8")), col.columnPath, consistency)
      Some(col.convertGetResult(result))
    } catch {
      case nfe: NotFoundException => None
    }
  }


  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[ResultType, ValueType](col: Gettable[ResultType, ValueType]): Option[ResultType] = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert[E](col: Column[E], consistency: Consistency) = detect {
    verifyInsert(col)
    verifyKeyspace(col.keyspace.value)
    client.insert(col.key.value, col.owner.asInstanceOf[ColumnContainer[_, _]].columnParent, col.cassandraColumn, consistency)
    col
  }

  
  /**
   * inserts the specified column value using the default consistency
   */
  def insert[E](col: Column[E]): Column[E] = insert(col, defaultConsistency)

  def add[E](col: CounterColumn[E], consistency: Consistency) = detect {
    verifyKeyspace(col.keyspace.value)
    client.add(col.key.value, col.owner.asInstanceOf[ColumnContainer[_, _]].columnParent, col.cassandraColumn, consistency)
    col
  }
  
  def add[E](col: CounterColumn[E]): CounterColumn[E] = add(col, defaultConsistency)

  /**
   *   counts the number of columns in the specified column container
   */
  def count(container: ColumnContainer[_, _], predicate:Predicate, consistency: Consistency): Int = detect {
    verifyKeyspace(container.keyspace.value)
    client.get_count(container.key.value, container.columnParent, predicate.slicePredicate, consistency)
  }

  /**
   *   counts the number of columns in the specified column container
   */
  def count(container: ColumnContainer[_, _], consistency: Consistency): Int = count(container, EmptyPredicate, consistency)
  /**
   * performs count on the specified column container
   */
  def count(container: ColumnContainer[_, _]): Int = count(container, defaultConsistency)

  def count[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]],
      predicate:Predicate = EmptyPredicate,
      consistency: Consistency = defaultConsistency): Map[ColumnContainer[ColumnType, ResultType], Int] = detect {
    
    if (containers.size > 0) detect {
      val firstContainer = containers(0)
      val keyspace = firstContainer.keyspace
      verifyKeyspace(keyspace.value)

      val keyStrings = containers.map {container => ByteBuffer.wrap(container.key.value.getBytes("UTF-8"))}
      val result = client.multiget_count(keyStrings, firstContainer.columnParent, predicate.slicePredicate, consistency)
      
      val containersByKey = containers.map {container =>
         (container.key.value, container)
      }.toMap
      
      result map {element =>
        (containersByKey(element._1), element._2.toInt)
      }
    } else {
      throw new IllegalArgumentException("must provide at least 1 container for a count(keys, predicate, consistency) call")
    }
  }
  
  /**
   * removes the specified column container
   */
  def remove(container: ColumnContainer[_, _], consistency: Consistency): Unit = detect {
    verifyKeyspace(container.keyspace.value)
    verifyRemove(container)
    client.remove(container.key.value, container.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(container: ColumnContainer[_, _]): Unit = remove(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(column: Column[_], consistency: Consistency): Unit = detect {
    verifyKeyspace(column.keyspace.value)
    client.remove(column.key.value, column.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column: CounterColumn[_]): Unit = remove(column, defaultConsistency)
  
  /**
   * removes the specified column container
   */
  def remove(column: CounterColumn[_], consistency: Consistency): Unit = detect {
    verifyKeyspace(column.keyspace.value)
    client.remove_counter(column.key.value, column.columnPath, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column: Column[_]): Unit = remove(column, defaultConsistency)


  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate, consistency: Consistency): ResultType = detect {
    verifyKeyspace(container.keyspace.value)
    val results = client.get_slice(container.key.value, container.columnParent, predicate.slicePredicate, consistency)
    container.convertListResult(convertList(results))
  }

  /**
   * performs a list of the specified container using no predicate and the default consistency.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType]): ResultType = list(container, EmptyPredicate, defaultConsistency)


  /**
   * performs a list of the specified container using the specified predicate value
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate): ResultType = list(container, predicate, defaultConsistency)


  /**
   * given a list of containers, will perform a slice retrieving all the columns specified
   * applying the provided predicate against those keys. Assumes that all the containers
   * generate the same columnParent. That is, if they are super columns, they all have the
   * same super column name (existing to separate key values), and regardless of column
   * container type - belong to the same column family. If they are not, the first key
   * in the sequence what is used in this query.<br>
   * NOTE (to be clear): If containers is a
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate, consistency: Consistency): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = {
    if (containers.size > 0) detect {
      val firstContainer = containers(0)
      val keyspace = firstContainer.keyspace
      val keyStrings = containers.map {container => ByteBuffer.wrap(container.key.value.getBytes("UTF-8"))}
      verifyKeyspace(keyspace.value)
      val results = client.multiget_slice(keyStrings, firstContainer.columnParent, predicate.slicePredicate, consistency)

      val containersByKey = containers.foldLeft(
        mutable.Map[String, ColumnContainer[ColumnType, ResultType]]())((acc, container) => {
          acc += (container.key.value -> container)
        })

      // def locate(key: String) = (containers.find {_.key.value.equals(key)}).get
      def locate(key: String) = containersByKey(key)

      convertMap(results).map { (tuple) =>
        val key   = locate(tuple._1)
        val value = key.convertListResult(tuple._2)
        (key -> value)
      }.toSeq
    } else {
      throw new IllegalArgumentException("must provide at least 1 container for a list(keys, predicate, consistency) call")
    }
  }


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]]): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, EmptyPredicate, defaultConsistency)


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, predicate, defaultConsistency)


  /**
   * performs a list on a key range in the specified column family. the predicate is applied
   * with the provided consistency guaranteed. the key range may be a range of keys or a range
   * of tokens. This list call is only available when using an order-preserving partition.
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, predicate: Predicate, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = detect {
    verifyKeyspace(family.keyspace.value)
    val results = client.get_range_slices(family.columnParent, predicate.slicePredicate, range.cassandraRange, consistency)
    var mapBuilder = LinkedHashMap.canBuildFrom[Key[ColumnType, ListType], ListType]()

    convertList(results).foreach { (keyslice) =>
      val key = (family \ keyslice.key)
      mapBuilder += (key -> key.convertListResult(keyslice.columns))
    }
    mapBuilder.result
  }


  /**
   * performs a key-range list without any predicate
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, consistency)
  }


  /**
   * performs a key-range list without any predicate, and using the default consistency
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, defaultConsistency)
  }

  
  def list[ColumnType, ListType](query: IndexQuery): Map[StandardKey, Seq[Column[StandardKey]]] = list(query, EmptyPredicate, defaultConsistency)
  
  def list[ColumnType, ListType](query: IndexQuery, predicate: Predicate, consistency: Consistency): Map[StandardKey, Seq[Column[StandardKey]]] = detect {
    val family: ColumnFamily[StandardKey] = query.family
    verifyKeyspace(family.keyspace.value)
    val results = client.get_indexed_slices(query.family.columnParent, query.indexClause, predicate.slicePredicate, consistency)
        
    var mapBuilder = LinkedHashMap.canBuildFrom[StandardKey, Seq[Column[StandardKey]]]()

    convertList(results).foreach { (keyslice) =>
      val key = (family \ keyslice.key)
      mapBuilder += (key -> key.convertListResult(keyslice.columns))
    }
    mapBuilder.result
  }
    
  /**
   * performs the specified seq of operations in batch. assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used.
   */
  def batch(ops: Seq[Operation], consistency: Consistency): Unit = {
    if (ops.size > 0) detect {
      val keyToFamilyMutations = new HashMap[ByteBuffer, JMap[String, JList[Mutation]]]()
      val keyspace = ops(0).keyspace
      verifyKeyspace(keyspace.value)

      def getOrElse[A, B](map: JMap[A, B], key: A, f: => B): B = {
        if (map.containsKey(key)) {
          map.get(key)
        } else {
          val newValue = f
          map.put(key, newValue)
          newValue
        }
      }

      ops.foreach {
        (op) =>
          verifyOperation(op)
          val familyToMutations = getOrElse(keyToFamilyMutations, ByteBuffer.wrap(op.key.value.getBytes("UTF-8")), new HashMap[String, JList[Mutation]]())
          val mutationList = getOrElse(familyToMutations, op.family.value, new ArrayList[Mutation]())
          mutationList.add(op.mutation)
      }

      // TODO may need to flatten duplicate super columns?
      client.batch_mutate(keyToFamilyMutations, consistency)
    } else {
      throw new IllegalArgumentException("cannot perform batch operation on 0 length operation sequence")
    }
  }

  /**
   * performs the list of operations in batch using the default consistency
   */
  def batch(ops: Seq[Operation]): Unit = batch(ops, defaultConsistency)


  /**
   * Performs the specified seq of operations in batch. Assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used. Will retry in the face of a Cassandra-related timeout exception.
   * TODO: Reforumulate where we keep splitting up the batch size into halves.
   */
  def batchWithRetry(ops: Seq[Operation], consistency: Consistency): Unit = timeoutTry {
    batch(ops, consistency)
  }


  /**
   * Performs the list of operations in batch using the default consistency, retrying in
   * the face of a Cassandra-related timeout exception.
   * TODO: Reforumulate where we keep splitting up the batch size into halves.
   */
  def batchWithRetry(ops: Seq[Operation]): Unit = timeoutTry { batch(ops) }


  /**
   * Given a code block, keep retrying it (up to maxTries) in the event of a
   * Cassandra-related timeout exception.
   */
  private def timeoutTry(f: =>Unit, maxTries:Int=5, timeoutWaitMsec:Int=200):Unit = {
    var tries = maxTries
    assert(maxTries > 0)
    while(tries > 0) {
      try { f; return } catch {
        case e:java.net.SocketTimeoutException => tries -= 1; if (tries <= 0) throw e
        case e:java.util.concurrent.TimeoutException => tries -= 1; if (tries <= 0) throw e
        case e:org.apache.cassandra.thrift.UnavailableException => tries -= 1; if (tries <= 0) throw e
        case e:Exception => throw e
      }
      Thread.sleep(timeoutWaitMsec)
    }
  }

  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c: Consistency): ConsistencyLevel = c.thriftValue

  /**
   * all calls which access the session should be wrapped within this method,
   * it will catch any exceptions and make sure the session is then removed
   * from the pool.
   */
  private def detect[T](f: => T) = try {
    f
  } catch {
    case t: Throwable => lastError = Some(t); throw t
  }

  private def Buffer[T](v:java.util.List[T]) = {
    scala.collection.JavaConversions.asScalaBuffer(v)
  }

  implicit private def convertList[T](v:java.util.List[T]):List[T] = {
    scala.collection.JavaConversions.asScalaBuffer(v).toList
  }

  implicit private def convertMap[K,V](v:java.util.Map[K,V]): scala.collection.mutable.Map[K,V] = {
    scala.collection.JavaConversions.mapAsScalaMap(v)
  }

  implicit private def convertSet[T](s:java.util.Set[T]):scala.collection.mutable.Set[T] = {
    scala.collection.JavaConversions.asScalaSet(s)
  }
}
