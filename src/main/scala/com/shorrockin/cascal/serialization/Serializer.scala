package com.shorrockin.cascal.serialization

import com.shorrockin.cascal.utils.{UUID => UUIDUtils, ThreadLocal}
import java.util.UUID
import java.util.Date
import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder
import java.nio.{ByteBuffer,CharBuffer}
import scala.util.DynamicVariable

object Serializer {

  /**
   * defines a map of all the default serializers
   */
  val Default = Map[Class[_], Serializer[_]](
    (classOf[String]  -> StringSerializer),
    (classOf[UUID]    -> UUIDSerializer),
    (classOf[Int]     -> IntSerializer),
    (classOf[Long]    -> LongSerializer),
    (classOf[Boolean] -> BooleanSerializer),
    (classOf[Float]   -> FloatSerializer),
    (classOf[Double]  -> DoubleSerializer),
    (classOf[Date]    -> DateSerializer)
  )
}

/**
 *  defines a class responsible for converting an object to and from an
 * array of bytes.
 *
 * @author Chris Shorrock
 */
trait Serializer[A] {
  /** converts this object to a byte array for entry into cassandra */
  def toByteBuffer(obj:A):ByteBuffer

  /** converts the specified byte array into an object */
  def fromByteBuffer(bytes:ByteBuffer):A

  /** converts the specified value to a string */
  def toString(obj:A):String

  /** converts the specified value from a string */
  def fromString(str:String):A
}

object StringSerializer extends Serializer[String] {
  val utf8 = Charset.forName("UTF-8")
  private val decoder = new ThreadLocal(utf8.newDecoder)
  private val encoder = new ThreadLocal(utf8.newEncoder)

  def toByteBuffer(str:String) = encoder.withValue {
    _.encode(CharBuffer.wrap(str.toCharArray))
  }
  def fromByteBuffer(bytes:ByteBuffer) = decoder.withValue {
    _.decode(bytes).toString
  }
  def toString(str:String) = str
  def fromString(str:String) = str
}

object UUIDSerializer extends Serializer[UUID] {
  def fromByteBuffer(bytes:ByteBuffer) = UUIDUtils(bytes.array)
  def toString(uuid:UUID) = uuid.toString
  def fromString(str:String) = UUID.fromString(str)

  def toByteBuffer(uuid:UUID) = {
    val msb = uuid.getMostSignificantBits()
    val lsb = uuid.getLeastSignificantBits()
    val buffer = new Array[Byte](16)

    (0 until 8).foreach  { (i) => buffer(i) = (msb >>> 8 * (7 - i)).asInstanceOf[Byte] }
    (8 until 16).foreach { (i) => buffer(i) = (lsb >>> 8 * (7 - i)).asInstanceOf[Byte] }

    ByteBuffer.wrap(buffer)
  }

}

object IntSerializer extends Serializer[Int] {
  val bytesPerInt = java.lang.Integer.SIZE / java.lang.Byte.SIZE

  def toByteBuffer(i:Int) = ByteBuffer.allocate(bytesPerInt).putInt(0, i)
  def fromByteBuffer(bytes:ByteBuffer) = bytes.getInt
  def toString(obj:Int) = obj.toString
  def fromString(str:String) = str.toInt
}

object LongSerializer extends Serializer[Long] {
  val bytesPerLong = java.lang.Long.SIZE / java.lang.Byte.SIZE

  def toByteBuffer(l:Long) = ByteBuffer.allocate(bytesPerLong).putLong(0, l)
  def fromByteBuffer(bytes:ByteBuffer) = bytes.getLong
  def toString(obj:Long) = obj.toString
  def fromString(str:String) = str.toLong
}

object BooleanSerializer extends Serializer[Boolean] {
  def toByteBuffer(b:Boolean) = StringSerializer.toByteBuffer(b.toString)
  def fromByteBuffer(bytes:ByteBuffer) = StringSerializer.fromByteBuffer(bytes).toBoolean
  def toString(obj:Boolean) = obj.toString
  def fromString(str:String) = str.toBoolean
}

object FloatSerializer extends Serializer[Float] {
  val bytesPerFloat = java.lang.Float.SIZE / java.lang.Byte.SIZE

  def toByteBuffer(f:Float) = ByteBuffer.allocate(bytesPerFloat).putFloat(0, f)
  def fromByteBuffer(bytes:ByteBuffer) = bytes.getFloat()
  def toString(obj:Float) = obj.toString
  def fromString(str:String) = str.toFloat
}

object DoubleSerializer extends Serializer[Double] {
  val bytesPerDouble = java.lang.Double.SIZE / java.lang.Byte.SIZE

  def toByteBuffer(d:Double) = ByteBuffer.allocate(bytesPerDouble).putDouble(0, d)
  def fromByteBuffer(bytes:ByteBuffer) = bytes.getDouble
  def toString(obj:Double) = obj.toString
  def fromString(str:String) = str.toDouble
}

object DateSerializer extends Serializer[Date] {
  def toByteBuffer(date:Date) = LongSerializer.toByteBuffer(date.getTime)
  def fromByteBuffer(bytes:ByteBuffer) = new Date(LongSerializer.fromByteBuffer(bytes).longValue)
  def toString(obj:Date) = obj.getTime.toString
  def fromString(str:String) = new Date(str.toLong.longValue)
}
