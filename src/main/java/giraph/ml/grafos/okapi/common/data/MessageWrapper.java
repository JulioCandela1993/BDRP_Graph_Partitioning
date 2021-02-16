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
package giraph.ml.grafos.okapi.common.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A common operation in several algorithms is for a vertex to send a message  
 * that also contains the sending vertex's ID. 
 * 
 * This class serves as a wrapper of such messages. It contains and ID as well
 * as the main payload of the message.
 * 
 * Users must subclass this abstract class and define what the classes of the
 * vertex ID and the main message are.
 * 
 * @param <I> SourceId
 * @param <M> Message
 */
@SuppressWarnings("rawtypes")
public abstract class MessageWrapper<
  I extends WritableComparable,
  M extends Writable>
  implements WritableComparable<MessageWrapper<I, M>> {
  /** Message sender vertex Id. */
  private I sourceId;
  /** Message with data. */
  private M message;

  /** Configuration. */
  private ImmutableClassesGiraphConfiguration <I, ?, ?> conf;

  /**
   * Using the default constructor requires that the user implement
   * setClass(), guaranteed to be invoked prior to instantiation in
   * readFields().
   */
  public MessageWrapper() {
  }

  /**
   * Constructor with another {@link MessageWrapper}.
   *
   * @param pMessageWrapper Message Wrapper to be used internally.
   *
   */
  public MessageWrapper(final I pSourceId, final M pMessage) {
    sourceId = pSourceId;
    message = pMessage;
  }

  /**
   * Subclasses must provide the vertex Id class type appropriately
   * and can use getVertexIdClass() to do it.
   *
   * @return Class
   */
  public abstract Class<I> getVertexIdClass();

  /**
   * Subclasses must provide the message class type appropriately
   * and can use getMessageClass() to do it.
   *
   * @return Class<M>
   */
  public abstract Class<M> getMessageClass();

  /**
   * Read Fields.
   *
   * @param input Input to be read.
   * @throws IOException for IO.
   */
  public void readFields(final DataInput input) throws IOException {
    sourceId = (I) ReflectionUtils.newInstance(getVertexIdClass(), conf);
    sourceId.readFields(input);
    message = (M) ReflectionUtils.newInstance(getMessageClass(), conf);
    message.readFields(input);
  }

  /**
   * Write Fields.
   *
   * @param output Output to be written.
   * @throws IOException for IO.
   */
  public void write(final DataOutput output) throws IOException {
    if (sourceId == null) {
      throw new IllegalStateException("write: Null source vertex index");
    }
    sourceId.write(output);
    message.write(output);
  }

  /**
   * Get Configuration.
   *
   * @return conf Configuration
   */
  final ImmutableClassesGiraphConfiguration<I, ?, ?> getConf() {
    return conf;
  }

  /**
   * Set Configuration.
   *
   * @param pConf Configuration to be stored.
   */
  final void setConf(final ImmutableClassesGiraphConfiguration<I, ?, ?> pConf) {
    conf = pConf;
  }

  /**
   * Return Vertex Source Id.
   *
   * @return sourceId Message sender vertex Id
   */
  public final I getSourceId() {
    return sourceId;
  }

  /**
   * Set Source Id.
   *
   * @param pSourceId Source Id to be set
   */
  public final void setSourceId(final I pSourceId) {
    sourceId = pSourceId;
  }

  /**
   * Return Message data.
   *
   * @return message message to be returned
   */
  public final M getMessage() {
    return message;
  }

  /**
   * Store message to this object.
   *
   * @param pMessage Message to be stored
   */
  public final void setMessage(final M pMessage) {
    message = pMessage;
  }

  /**
   * Return Message to the form of a String.
   *
   * @return String object
   */
  public String toString() {
    return "MessageWrapper{"
      + ", sourceId=" + sourceId
      + ", message=" + message
      + '}';
  }

  /**
   * CompareTo method.
   *
   * @param wrapper WRapper to be compared to
   *
   * @return 0 if equal
   */
  public final int compareTo(final MessageWrapper<I, M> wrapper) {

    if (this == wrapper) {
      return 0;
    }

    if (((Comparable<I>) sourceId).compareTo(
      (I) wrapper.getSourceId()) == 0) {
      return ((Comparable<M>) message).compareTo(
        (M) wrapper.getMessage());
    } else {
      return ((Comparable<I>) sourceId).compareTo(
        (I) wrapper.getSourceId());
    }
  }

  /**
   * Check if object is equal to message.
   *
   * @param other Object to be checked
   *
   * @return boolean value
   */
  public final boolean equals(final MessageWrapper<I, M> other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    MessageWrapper<I, M> that = other;

    if (message != null ? !message.equals(that.message) :
        that.message != null) {
      return false;
    }
    if (sourceId != null ? !sourceId.equals(that.sourceId) :
        that.sourceId != null) {
      return false;
    }
    return true;
  }
}