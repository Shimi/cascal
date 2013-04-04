package com.shorrockin.cascal

import scala.language.implicitConversions
import org.junit.{Assert, Test}
import com.shorrockin.cascal.utils.Conversions._
import com.shorrockin.cascal.serialization.TupleSerializer
import Assert._
import com.shorrockin.cascal.session.Insert
import com.shorrockin.cascal.session.RangePredicate
import com.shorrockin.cascal.session.Order

class CompositeTest extends EmbeddedCassandra {
  
  @Test def composite2InsertGet = borrow { session =>
    val name = ("composite name", 1)
    val col = "Test" \ "Composite2" \ "Insert Get" \ name
    session.insert(col \ "composite value")
    
    val colName = session.get(col).get.name
    assertEquals(name, tuple[String, Int](session.get(col).get.name))
  }
  
  @Test def composite3InsertGet = borrow { session =>
    val name = ("composite name", 1L, "name part 3")
    val col = "Test" \ "Composite3" \ "Insert Get" \ name
    session.insert(col \ "composite value")
    
    val colName = session.get(col).get.name
    assertEquals(name, tuple[String, Long, String](session.get(col).get.name))
  }

  @Test def composite2Range = borrow { session =>
    val key = "Test" \ "Composite2" \ "Composite Range"
    val col1 = key \ (("composite", 1)) \ 1
    val col2 = key \ (("composite", 2)) \ 1
    val col3 = key \ (("composite", 3)) \ 1
    val col4 = key \ (("composite", 4)) \ 1
    val col5 = key \ (("composite", 5)) \ 1
    val col6 = key \ (("comcom", 5)) \ 1
    
    session.batch(Insert(col1) :: Insert(col2) :: Insert(col3) :: Insert(col4) :: Insert(col5) :: Insert(col6))
    
    val result1 = session.list(key, RangePredicate(Some(("c", None)), None, Order.Ascending, None))
    assertEquals(6, result1.size)
    
    val result2 = session.list(key, RangePredicate(Some(("composite", None)), None, Order.Ascending, None))
    assertEquals(5, result2.size)
    
    val result3 = session.list(key, RangePredicate(("composite", 2), ("composite", 4)))
    assertEquals(3, result3.size)
    assertEquals(col2, result3(0))
    assertEquals(col3, result3(1))
    assertEquals(col4, result3(2))
  }
  
  @Test def composite3Range = borrow { session =>
    val key = "Test" \ "Composite3" \ "Composite Range"
    val col1 = key \ (("composite", 1L, "A")) \ 1
    val col2 = key \ (("composite", 1L, "a")) \ 1
    val col3 = key \ (("composite", 10L, "A")) \ 1
    
    session.batch(Insert(col1) :: Insert(col2) :: Insert(col3))
    
    val result0 = session.list(key, RangePredicate(Some(("composite", None)), None, Order.Descending, None))
    assertEquals(0, result0.size)
    
    val result1 = session.list(key, RangePredicate(Some(("composite", None)), None, Order.Ascending, None))
    assertEquals(3, result1.size)
    
    val result2 = session.list(key, RangePredicate(("composite", 1L), ("composite", 2L)))
    assertEquals(2, result2.size)
    assertEquals(col1, result2(0))
    assertEquals(col2, result2(1))
    
    val result3 = session.list(key, RangePredicate(("composite", 1L, "B"), ("composite", 2L)))
    assertEquals(1, result3.size)
    assertEquals(col2, result3(0))
  }
}