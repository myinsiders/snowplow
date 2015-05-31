/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class IgluAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the IgluAdapter functionality"                                                            ^
                                                                                                                            p^
  "toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is minimally populated"             ! e1^
  "toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is maximally populated"             ! e2^
  "toRawEvents should return a NEL containing one RawEvent if the Clojure-Tomcat querystring is populated"                   ! e3^
  "toRawEvents should return a NEL containing one RawEvent with a non-app platform if specified"                             ! e4^
  "toRawEvents should return a Validation Failure if there are no parameters on the CloudFront querystring"                  ! e5^
  "toRawEvents should return a Validation Failure if there is no schema parameter on the CloudFront querystring"             ! e6^
  "toRawEvents should return a Validation Failure if the schema parameter is not in an Iglu-compatible format"               ! e7^
  "toRawEvents should not throw NullPointerException for strange values"               ! e8^
                                                                                                                             end

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.snowplowanalytics.iglu", "v1")
    val cfSource = CollectorSource("cloudfront", "UTF-8", None)
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.snowplowanalytics.iglu-v1",
      "e"  -> "ue"
      )
    val static = staticNoPlatform ++ Map(
      "p"  -> "app"
    )
  }

  def e1 = {
    val params = toNameValuePairs(
      "schema"         -> "iglu:com.acme/campaign/jsonschema/1-0-0",
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "download",
      "publisher_name" -> "Organic",
      "source"         -> "",
      "tracking_id"    -> "",
      "ad_unit"        -> ""
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.acme/campaign/jsonschema/1-0-0",
              |"data":{
                |"name":"download",
                |"source":null,
                |"ad_unit":null,
                |"tracking_id":null,
                |"publisher_name":"Organic",
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ Map("ue_pr" -> expectedJson), None, Shared.cfSource, Shared.context)))
  }

  def e2 = {
    val params = toNameValuePairs(
      "schema"         -> "iglu:com.acme/campaign/jsonschema/1-0-0",
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "install",
      "source"         -> "newsfeed",
      "tracking_id"    -> "3353af9c-e298-2cf3-9f1c-b377a9c84dad",
      "ad_unit"        -> "UN-11-b",
      "aid"            -> "webhooks"
      )    
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)
    
    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.acme/campaign/jsonschema/1-0-0",
                |"data":{
                  |"name":"install",
                  |"source":"newsfeed",
                  |"ad_unit":"UN-11-b",
                  |"tracking_id":"3353af9c-e298-2cf3-9f1c-b377a9c84dad",
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]","")
      Map(
        "ue_pr" -> json,
        "aid"   -> "webhooks"
      )
    }

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cfSource, Shared.context)))
  }

  def e3 = {
    val params = toNameValuePairs(
      "schema"         -> "iglu:com.acme/campaign/jsonschema/2-0-0",
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "retarget",
      "source"         -> "newsfeed",
      "tracking_id"    -> "",
      "ad_unit"        -> "UN-11-b",
      "aid"            -> "my webhook project",
      "cv"             -> "clj-0.5.0-tom-0.0.4",
      "nuid"           -> ""
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cljSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)
    
    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.acme/campaign/jsonschema/2-0-0",
                |"data":{
                  |"name":"retarget",
                  |"source":"newsfeed",
                  |"ad_unit":"UN-11-b",
                  |"tracking_id":null,
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]","")
      Map(
        "ue_pr" -> json,
        "aid"   -> "my webhook project",
        "cv"    -> "clj-0.5.0-tom-0.0.4",
        "nuid"  -> ""
      )
    }

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cljSource, Shared.context)))
  }

  def e4 = {
    val params = toNameValuePairs(
      "schema"         -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "user"           -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name"           -> "download",
      "p"              -> "mob"
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.acme/campaign/jsonschema/1-0-1",
              |"data":{
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac",
                |"name":"download"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.staticNoPlatform ++ Map("p" -> "mob", "ue_pr" -> expectedJson), None, Shared.cfSource, Shared.context)))
  }

  def e5 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Querystring is empty: no Iglu-compatible event to process"))
  }

  def e6 = {
    val params = toNameValuePairs(
      "some_param"     -> "foo",
      "p"              -> "mob"
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Querystring does not contain schema parameter: not an Iglu-compatible self-describing event"))
  }

  def e7 = {
    val params = toNameValuePairs(
      "schema"         -> "iglooooooo://blah"
      )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("iglooooooo://blah is not a valid Iglu-format schema URI"))
  }

  def e8 = {
    //CollectorPayload(CollectorApi(com.snowplowanalytics.iglu,v1),List(schema=iglu:com.au.digdeep/cadreon/jsonschema/1-0-0, event_type=impression,
    // tp_user_id=4727379f-231a-4471-9b09-6804b3013f75, tp_campaign_name=AU_Industry Super_EOFY Maintenance_May-Aug 2015_INUEOF,
    // tp_publisher_name=Cadreon AU, tp_section_name=General Interest, Predictive Targeting ,  Retargeting,
    // tp_placement_name=Cadreon_General Interest, Predictive Targeting ,  Retargeting_728x90_Stdbnr, dd_device_id=362b8511891baeace742ea5108deca6763072c0a0402173224dcfeac0e016c05e88c6b3c58e25fb1dd2de4cf3aefde58, dd_is_mobile=false, dd_device_type=Desktop, dd_web_traffic_tor=false, dd_web_traffic_bot=false, cv=clj-0.9.1-tom-0.1.0, nuid=fc0d2afb-6b85-43f3-889c-b95121304d49),None,None,CollectorSource(clj-tomcat,UTF-8,None),CollectorContext(2015-05-29T01:33:25.000Z,Some(220.244.25.102),Some(Mozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F43.0.2357.81+Safari%2F537.36),Some(http://ad.doubleclick.net/N2455/adi/Global_Footer;sz=728x90;dc_ref=http%3A%2F%2Fwww.ebay.com.au;passback=pbk;ord=[timestamp]?),List(),None))
    val params = toNameValuePairs(
      "schema"         -> "iglu:com.au.digdeep/cadreon/jsonschema/1-0-0",
      "event_type"           -> "impression",
      "tp_campaign_name"           -> "AU_Industry Super_EOFY Maintenance_May-Aug 2015_INUEOF",
      "tp_publisher_name"              -> "Cadreon AU, tp_section_name=General Interest, Predictive Targeting ,  Retargeting",
      "tp_placement_name" -> "Cadreon_General Interest, Predictive Targeting ,  Retargeting_728x90_Stdbnr"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload)
    println(actual)
    val expectedJson =
      """|{
        |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
        |"data":{
        |"schema":"iglu:com.acme/campaign/jsonschema/1-0-1",
        |"data":{
        |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac",
        |"name":"download"
        |}
        |}
        |}""".stripMargin.replaceAll("[\n\r]","")

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Expected.staticNoPlatform ++ Map("p" -> "mob", "ue_pr" -> expectedJson), None, Shared.cfSource, Shared.context)))
  }
}
