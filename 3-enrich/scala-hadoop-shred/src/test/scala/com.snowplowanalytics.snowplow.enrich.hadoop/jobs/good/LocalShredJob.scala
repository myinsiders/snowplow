/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package hadoop
package jobs
package good

// Scalding


import java.io.File

import com.twitter.scalding._


// Cascading
import cascading.tuple.TupleEntry
import cascading.tap.SinkMode

// This project
import JobSpecHelpers._

// Specs2
import org.specs2.mutable.Specification

/**
 * Integration test for the EtlJob:
 *
 * The enriched event contains a JSON
 * which should pass validation.
 */
class LocalShredJob extends Specification {

  "A job which is provided with a pre-defined enriched input" should {

    val Sinks =
      JobSpecHelpers.runJobInTool(new File(classOf[LocalShredJob].getResource("/enriched_input").getFile))

    "output must be of expected size" in {
      import java.io.File
//      // Scala
      import scala.io.{Source => ScalaSource}
//      linkClickEventSource.getLines.toList mustEqual Seq(LinkClickEventSpec.expected.contents)
      println("Good in " + Sinks.output)
      ScalaSource.fromFile(new File(Sinks.output, "com.au.digdeep/cadreon/jsonschema/1-0-0")).getLines.toList.size must_== 1143
    }

    "filtered must be of expected size" in {
      // Java
      import java.io.File
      // Scala
      import scala.io.{Source => ScalaSource}
      println("Filtered in " + Sinks.filtered)
      ScalaSource.fromFile(Sinks.filtered).getLines.toList.size must_== 1147
    }
    "not trap any exceptions" in {
      // TODO: not working
      Sinks.exceptions must beEmptyDir
    }
    "not write any bad row JSONs" in {
      // TODO: not working
      Sinks.badRows must beEmptyDir
    }

//     Sinks.deleteAll()
    ()
  }
}
