package com.scala.examples

object PalindromeExample {
  def isPalindrome(someNumber:String):Boolean= {
    val len = someNumber.length
    for(i<- 0 until len/2){
      if(someNumber(i) != someNumber(len-i-1))
        return false
    }
    return true
  }

  def main(args: Array[String]): Unit = {

    println("Palindrome::"+isPalindrome("234432"))
  }


}
