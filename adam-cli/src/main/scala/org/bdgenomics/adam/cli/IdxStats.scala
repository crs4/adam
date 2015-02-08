/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{ Projection, AlignmentRecordField }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.formats.avro.Contig
import org.kohsuke.args4j.Argument

object IdxStats extends ADAMCommandCompanion {
  val commandName: String = "idxstats"
  val commandDescription: String = "Print statistics on reads distribution in an ADAM file (similar to samtools idxstats)"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new IdxStats(Args4j[IdxStatsArgs](cmdLine))
  }
}

class IdxStatsArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM data to return stats for", index = 0)
  val inputPath: String = null
}

class IdxStats(protected val args: IdxStatsArgs) extends ADAMSparkCommand[IdxStatsArgs] {
  val companion: ADAMCommandCompanion = IdxStats

  def run(sc: SparkContext, job: Job): Unit = {

    val projection = Projection(
      AlignmentRecordField.readMapped,
      AlignmentRecordField.mateMapped,
      AlignmentRecordField.contig,
      AlignmentRecordField.mateContig)

    val adamFile: RDD[AlignmentRecord] = sc.loadAlignments(args.inputPath, projection = Some(projection))

    val hits = adamFile.adamIdxStats()

    // FIXME will I need this?
    def percent(fraction: Long, total: Long) = if (total == 0) 0.0 else 100.00 * fraction.toFloat / total

    for (h <- hits) {
      if (h._1 == null) { println("There are %d reads mapped to null".format(h._2)) }
      else println("%s - %d".format(h._1.contigName, h._2))
    }
  }
}
