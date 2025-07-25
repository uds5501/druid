/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.querykit.results;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.querykit.FrameProcessorTestBase;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueryResultsFrameProcessorTest extends FrameProcessorTestBase
{
  @Test
  public void sanityTest() throws ExecutionException, InterruptedException, IOException
  {

    final IncrementalIndexCursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .maxRowsPerFrame(5)
                            .frameType(FrameType.latestRowBased())
                            .allocator(ArenaMemoryAllocator.createOnHeap(100_000));

    final RowSignature signature = frameSequenceBuilder.signature();
    final List<Frame> frames = frameSequenceBuilder.frames().toList();
    final BlockingQueueFrameChannel inputChannel = new BlockingQueueFrameChannel(frames.size());
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    try (final WritableFrameChannel writableInputChannel = inputChannel.writable()) {
      for (final Frame frame : frames) {
        writableInputChannel.write(frame);
      }
    }

    final StagePartition stagePartition = new StagePartition(new StageId("query", 0), 0);

    final QueryResultsFrameProcessor processor =
        new QueryResultsFrameProcessor(
            ReadableInput.channel(
                inputChannel.readable(),
                FrameReader.create(signature),
                stagePartition
            ).getChannel(),
            outputChannel.writable()
        );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);
    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    );
    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory, signature, false),
        rowsFromProcessor
    );
    Assert.assertEquals(Unit.instance(), retVal.get());
  }

}
