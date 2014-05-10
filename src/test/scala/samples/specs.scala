package samples

import org.specs2.mutable._

class BasicSpec extends Specification {

    "test group" should {
      "sub test" in {
        "Hi" must startWith("Hi")
      }
    }
  }