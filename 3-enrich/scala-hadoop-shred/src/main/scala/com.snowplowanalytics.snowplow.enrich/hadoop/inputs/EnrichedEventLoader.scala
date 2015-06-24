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
package inputs

// Java
import java.util.UUID

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Scala
import scala.util.Try

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Utils
import com.snowplowanalytics.util.Tap._

// Snowplow Common Enrich
import common._
import outputs.EnrichedEvent

/**
 * A loader for Snowplow enriched events - i.e. the
 * TSV files generated by the Snowplow Enrichment
 * process.
 */
object EnrichedEventLoader {

  def main(args: Array[String]): Unit = {
    val json = "{\n    \"schema\": \"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\n    \"data\": [\n        {\n            \"schema\": \"iglu:com.au.digdeep/page/jsonschema/2-0-0\",\n            \"data\": {\n                \"WPtemplateFile\": \"undefined\",\n                \"WPtemplateDisplay\": \"undefined\",\n                \"WPtemplateDisplayType\": \"undefined\",\n                \"WPtemplateFileId\": \"undefined\",\n                \"WPtemplateFilePath\": \"/\",\n                \"WPappId\": \"undefined\",\n                \"WPeventId\": \"undefined\"\n            }\n        }\n    ]\n}"
    val json2 = "{\n    \"schema\": \"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\n    \"data\":         {\n            \"schema\": \"iglu:com.au.digdeep/page/jsonschema/2-0-0\",\n            \"data\": {\n                \"WPtemplateFile\": \"undefined\",\n                \"WPtemplateDisplay\": \"undefined\",\n                \"WPtemplateDisplayType\": \"undefined\",\n                \"WPtemplateFileId\": \"undefined\",\n                \"WPtemplateFilePath\": \"/\",\n                \"WPappId\": \"undefined\",\n                \"WPeventId\": \"undefined\"\n            }\n            }\n}"
    val contextsJson = Mapper.readTree(json)
    val dataNode = contextsJson.at("/data")
    if (dataNode.isMissingNode) return
    def processDataNode(dataNode: JsonNode): Any = {
      val device_id = dataNode.at("/data/WPtemplateFile")
      val user_id = dataNode.at("/data/augurUID")
      (device_id.isMissingNode, user_id.isMissingNode) match {
        case (true, _) => None
        case (false, true) => println(device_id.textValue(), null)
        case (false, false) => println(device_id.textValue(), user_id.textValue())
      }
    }
    if (dataNode.isArray) {
      for (index <- Range(0, dataNode.size)) {
        val childNode = dataNode.get(index)
        processDataNode(childNode)
      }
    } else {
      processDataNode(dataNode)
    }
  }


  private val RedshiftTstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)
  private lazy val Mapper = new ObjectMapper
  private val FieldCount = 108

  private object FieldIndexes { // 0-indexed
    val collectorTstamp = 3
    val eventId = 6
    val contexts = 52
    val unstructEvent = 58
    val network_userId = 17
    val user_fingerprint = 14
    val augur_did = 109
    val augur_user_id = 110
  }

  def extractAugur(fields: Array[String]): Option[(String, String)] = {
    val contexts = fields(FieldIndexes.contexts)
    val unstruct = fields(FieldIndexes.unstructEvent)

    val nodes = List(contexts, unstruct).flatMap { json =>
      if (json != null) {
        try {
          val contextsJson = Mapper.readTree(json)
          val dataNode = contextsJson.at("/data")
          if (!dataNode.isMissingNode) {
            dataNode.isArray match {
              case true => Range(0, dataNode.size).map(index => dataNode.get(index))
              case false => List(dataNode)
            }
          } else {
            List()
          }
        }
        catch {
          case t: Throwable =>
            t.printStackTrace()
            List()
        }
      } else {
        List()
      }
    }

    nodes.flatMap { node =>
      val device_id = node.at("/data/augurDID")
      val user_id = node.at("/data/augurUID")
      (device_id.isMissingNode, user_id.isMissingNode) match {
        case (true, _) => None
        case (false, true) => Some(device_id.textValue(), null)
        case (false, false) => Some(device_id.textValue(), user_id.textValue())
      }
    }.headOption
  }

  def filterFields(fields: Array[String]): Array[String] = {
    try {
      val newFields = fields.clone()
      newFields(FieldIndexes.contexts) = null
      newFields(FieldIndexes.unstructEvent) = null
      newFields(FieldIndexes.network_userId) = null
      newFields(FieldIndexes.user_fingerprint) = null
      val deviceDetails = extractAugur(fields)
      if (deviceDetails.isDefined) {
        val (device_id, user_id) = deviceDetails.get
        newFields ++ Array(device_id, user_id)
      } else {
        newFields ++ Array(null, null)
      }
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        fields ++ Array(null, null)
    }
  }


  /**
   * Converts the source string into a 
   * ValidatedEnrichedEvent. Note that
   * this loads the bare minimum required
   * for shredding - basically four fields.
   *
   * @param line A line of data to convert
   * @return either a set of validation
   *         Failures or a EnrichedEvent
   *         Success.
   */
  // TODO: potentially in the future this could be replaced by some
  // kind of Scalding pack()
  def toEnrichedEvent(line: String): ValidatedEnrichedEvent = {

    val fields = line.split("\t", -1).map(f => if (f == "") null else f)

    toEnrichedEvent(fields)
  }

  def toEnrichedEvent(fields: Array[String]) : ValidatedEnrichedEvent = {
    val len = fields.length
    if (len < FieldCount)
      return s"Line does not match Snowplow enriched event (expected ${FieldCount}+ fields; found $len)".failNel[EnrichedEvent]

    val event = new EnrichedEvent().tap { e =>
      e.contexts = fields(FieldIndexes.contexts)
      e.unstruct_event = fields(FieldIndexes.unstructEvent)
    }

    // Get and validate the event ID
    val eventId = validateUuid("event_id", fields(FieldIndexes.eventId))
    for (id <- eventId) { event.event_id = id }

    // Get and validate the collector timestamp
    val collectorTstamp = validateTimestamp("collector_tstamp", fields(FieldIndexes.collectorTstamp))
    for (tstamp <- collectorTstamp) { event.collector_tstamp = tstamp }

    (eventId.toValidationNel |@| collectorTstamp.toValidationNel) {
      (_,_) => event
    }
  }

  /**
   * Validates that the given field contains a valid UUID.
   *
   * @param field The name of the field being validated
   * @param str The String hopefully containing a UUID
   * @return a Scalaz ValidatedString containing either
   *         the original String on Success, or an error
   *         String on Failure.
   */
  private def validateUuid(field: String, str: String): ValidatedString = {

    def check(s: String)(u: UUID): Boolean = (u != null && s == u.toString)
    val uuid = Try(UUID.fromString(str)).toOption.filter(check(str))
    uuid match {
      case Some(_) => str.success
      case None    => s"Field [$field]: [$str] is not a valid UUID".fail
    }
  }

  /**
   * Validates that the given field contains a valid
   * (Redshift/Postgres-compatible) timestamp.
   *
   * @param field The name of the field being validated
   * @param str The String hopefully containing a
   *        Redshift/PG-compatible timestamp
   * @return a Scalaz ValidatedString containing either
   *         the original String on Success, or an error
   *         String on Failure.
   */
  private def validateTimestamp(field: String, str: String): ValidatedString =
    try {
      val _ = RedshiftTstampFormat.parseDateTime(str)
      str.success
    } catch {
      case e: Throwable =>
        s"Field [$field]: [$str] is not in the expected Redshift/Postgres timestamp format".fail
    }
}
