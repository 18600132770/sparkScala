package com.huag.core

/**
  * 二次排序
  * @author huag
  * @date 2019/12/11 9:43
  */
class SecondSortKey(val first:Int, val second:Int) extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    if(this.first != that.first){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}
