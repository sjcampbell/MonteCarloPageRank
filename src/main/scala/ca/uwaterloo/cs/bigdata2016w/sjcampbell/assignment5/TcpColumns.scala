package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment5

object LineItemColumns {
    final val orderKey = 0
    final val partKey = 1
    final val suppKey = 2
    final val lineNumber = 3
    final val quantity = 4
    final val extendedPrice = 5
    final val discount = 6
    final val tax = 7
    final val returnFlag = 8
    final val lineStatus = 9
    final val shipDate = 10
    final val commitDate = 11
    final val receiptDate = 12
    final val shipInstruct = 13
    final val shipMode = 14
    final val comment = 15
}

object OrderColumns {
    final val orderKey = 0
    final val custKey = 1
    final val orderStatus = 2
    final val totalPrice = 3
    final val orderDate = 4
    final val orderPriority = 5
    final val clerk = 6
    final val shipPriority = 7
    final val comment = 8
}

object CustomerColumns {
    final val custKey = 0
    final val name = 1
    final val address = 2
    final val nationkey = 3
    final val acctbal = 4
    final val mktsegment = 5
    final val comment = 6
}