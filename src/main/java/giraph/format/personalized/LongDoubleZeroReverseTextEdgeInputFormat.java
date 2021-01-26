/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package giraph.format.personalized;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with long IDs and double values. 
 * 
 * Note that this version also creates the reverse edges.
 *
 * Each line consists of: <source id> <target id>
 * Reads an edge and also creates its reverse.
 * This format reads the src and trg ids and adds a zero weight on the edge.
 *
 */
public class LongDoubleZeroReverseTextEdgeInputFormat
    extends LongDoubleZerosTextEdgeInputFormat {
  @Override
  public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    EdgeReader<LongWritable, DoubleWritable> edgeReader =
        super.createEdgeReader(split, context);
    edgeReader.setConf(getConf());
    return new ReverseEdgeDuplicator<LongWritable, DoubleWritable>(edgeReader);
  }
}