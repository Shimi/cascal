package com.shorrockin.cascal.model

import scala.language.implicitConversions
import org.apache.cassandra.thrift.IndexClause
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{IndexExpression => CassIndexExpression}
import org.apache.cassandra.thrift.{IndexOperator => ThriftOperator}
import scala.collection.JavaConversions._

case class IndexExpression(val columnName: ByteBuffer, val operator: ThriftOperator, val value: ByteBuffer)

case class IndexQuery(val family: ColumnFamily[StandardKey], val expressions: List[IndexExpression],
    val startKey: ByteBuffer, val limit: Int = 100) {
  
  val indexClause = new IndexClause();
  
  implicit def CascalIndexExpression(expression: IndexExpression): CassIndexExpression =
	  new CassIndexExpression(expression.columnName, expression.operator, expression.value)
  
  implicit def CascalIndexExpressionList(expressions: List[IndexExpression]): List[CassIndexExpression] =
	  expressions.map(CascalIndexExpression(_))
	  
  var ex = CascalIndexExpressionList(expressions)
  indexClause.setExpressions(ex)
  indexClause.setStart_key(startKey)
  indexClause.setCount(limit)
  
  def limit(limit: Int) = IndexQuery(family, expressions, startKey, limit)
}

object IndexQuery {
  
  class IndexExpressionHelper(val queryHelper: IndexQueryHelper,
      val colName: ByteBuffer) {
    
    def startAt(startAT: ByteBuffer): IndexQuery = {
      queryHelper.startAt(startAT)
    }
    
    private def thriftOperator(value: ByteBuffer, operator: ThriftOperator) = {
      val expression = IndexExpression(colName, operator, value)
	  queryHelper += expression
	  queryHelper
    }
    
	def Eq(value: ByteBuffer) = thriftOperator(value, ThriftOperator.EQ)
	def Gt(value: ByteBuffer) = thriftOperator(value, ThriftOperator.GT)
	def Gte(value: ByteBuffer) = thriftOperator(value, ThriftOperator.GTE)
	def Lt(value: ByteBuffer) = thriftOperator(value, ThriftOperator.LT)
	def Lte(value: ByteBuffer) = thriftOperator(value, ThriftOperator.LTE)
  }
  
  class IndexQueryHelper(val family: ColumnFamily[StandardKey]) {
    
    var expressions: List[IndexExpression] = List()
    
    def startAt(startAt: ByteBuffer) = IndexQuery(family, expressions, startAt)
    
    def and(columnName: ByteBuffer) = new IndexExpressionHelper(this, columnName)
    
    def +=(expression: IndexExpression) = expressions = expression :: expressions
  }
  
  def indexQueryHelper(columnFamily: ColumnFamily[StandardKey], columnName: ByteBuffer) = new IndexExpressionHelper(new IndexQueryHelper(columnFamily), columnName)
  
}