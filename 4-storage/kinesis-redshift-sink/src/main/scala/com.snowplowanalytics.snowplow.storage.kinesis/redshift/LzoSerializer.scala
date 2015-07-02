/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import com.snowplowanalytics.snowplow.storage.kinesis.Redshift.EmitterInput

// Java libs
import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}

// Java lzo
import com.hadoop.compression.lzo.LzopCodec
import org.apache.hadoop.conf.Configuration

// Elephant bird
import com.twitter.elephantbird.mapreduce.io.RawBlockWriter

// Scalaz
import scalaz.Scalaz._
import scalaz._

// Logging
import org.apache.commons.logging.LogFactory

// AWS libs

// AWS Kinesis connector libs

/**
 * Object to handle LZO compression of raw events
 */
object LzoSerializer {

  val log = LogFactory.getLog(getClass)

  val lzoCodec = new LzopCodec()
  val conf = new Configuration()
  conf.set("io.compression.codecs", classOf[LzopCodec].getName)
  lzoCodec.setConf(conf)

  /**
   * Compress a list of Snowplow events
   *
   * @param records List of deserialized records
   * @return Tuple4 containing: the output stream for the .lzo file
   *                            the output stream for the .lzo.index file
   *                            the compression codec
   *                            the list of events
   */
  def serialize(records: List[ EmitterInput ]): (ByteArrayOutputStream, ByteArrayOutputStream, LzopCodec, List[EmitterInput]) = {

    val indexOutputStream = new ByteArrayOutputStream()
    val outputStream = new ByteArrayOutputStream()

    // This writes to the underlying outputstream and indexoutput stream
    val lzoOutputStream = lzoCodec.createIndexedOutputStream(outputStream, new DataOutputStream(indexOutputStream))

    val rawBlockWriter = new RawBlockWriter(lzoOutputStream)

    // Populate the output stream with records
    val results = records.map({ record => try {
        (record._1, record._2.map(r => {
          rawBlockWriter.write(r)
          r
        }))
      } catch {
        case e: IOException => {
          log.warn(e)
          (record._1, List("Error writing raw event to output stream: [%s]".format(e.toString)).fail)
        }
      }
    })

    rawBlockWriter.close

    (outputStream, indexOutputStream, lzoCodec, results)
  }
}
