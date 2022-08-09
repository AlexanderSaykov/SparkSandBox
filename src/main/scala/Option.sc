val values = List("1", "2", "foo")

def cleverInt(s: String): Option[Int] = {
  try {Some(s.toInt)}
  catch {case e: Exception => None}
}

println(values.map(cleverInt))

def getOrZero(x: Option[Int]) = {x match {
  case Some(v) => v
  case None => 0
}}

println(values.flatMap(cleverInt))




