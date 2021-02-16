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

package giraph.lri.rrojas.rankdegree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import giraph.ml.grafos.okapi.common.data.LongArrayListWritable;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.Writable;

public class SamplingMessage implements Writable {
	private int sourceId;
	private int partition;
	private LongArrayListWritable friendlist;

	public SamplingMessage() {
	}

	public SamplingMessage(int sourceId, int partition) {
		this.sourceId = sourceId;
		this.partition = partition;
		this.friendlist = new LongArrayListWritable();
	}

	public SamplingMessage(int sourceId, LongArrayListWritable friendlist) {
		this.sourceId = sourceId;
		this.partition = -1;
		this.friendlist = friendlist;
	}



	public int getSourceId() {
		return sourceId;
	}

	public void setSourceId(int sourceId) {
		this.sourceId = sourceId;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}


	public LongArrayListWritable getFriendlist() {
		return friendlist;
	}

	public void setFriendlist(LongArrayListWritable friendlist) {
		this.friendlist = friendlist;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		sourceId = input.readInt();
		partition = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(sourceId);
		output.writeInt(partition);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SamplingMessage that = (SamplingMessage) o;
		if (partition != that.partition || sourceId != that.sourceId) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return getSourceId() + " " + getPartition();
	}
}