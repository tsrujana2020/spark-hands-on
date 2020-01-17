package com.scala.examples

object PrimeFactors {

     def main(args : Array[String]) {
      for (n <- 1 to 30) {
        val d = smallestDivisor(n)
        println(n + ": lcd = " + d + (if (n == d) " prime!" else " not prime") )
      }
    }

    def isPrime(n : Int) : Boolean = {
      n == smallestDivisor(n)
    }

    def smallestDivisor(n : Int) : Int = {
      findDivisor(n, 2)
    }

    def findDivisor(n : Int, testDivisor : Int) : Int = {
      if (square(testDivisor) > n)
        n
      else if (divides(testDivisor, n))
        testDivisor
      else
        findDivisor(n, testDivisor + 1)
    }
    def square(n : Int) : Int = {
      n * n
    }
    def divides(d : Int, n : Int) : Boolean = {
      (n % d) == 0
    }

}
