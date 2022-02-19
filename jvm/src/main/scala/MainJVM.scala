import mainshared.MainShared

object MainJVM {
  def main(args: Array[String]): Unit = {
    println("""JVMJVM running from directly within the jvm folder""")
    MainShared.mainShared()
  }
}
