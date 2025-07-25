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

package org.apache.druid.frame.write;

import com.google.common.collect.Iterables;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.field.TransformUtilsTest;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyComparator;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tests that exercise {@link FrameWriter} implementations.
 */
@RunWith(Parameterized.class)
public class FrameWriterTest extends InitializedNullHandlingTest
{

  static {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  private static final int DEFAULT_ALLOCATOR_CAPACITY = 1_000_000;

  @Nullable
  private final FrameType inputFrameType;
  private final FrameType outputFrameType;
  private final KeyOrder sortedness;

  private MemoryAllocator allocator;

  @Nullable
  private Consumer<ColumnCapabilitiesImpl> capabilitiesAdjustFn;

  public FrameWriterTest(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final KeyOrder sortedness
  )
  {
    this.inputFrameType = inputFrameType;
    this.outputFrameType = outputFrameType;
    this.sortedness = sortedness;
    this.allocator = ArenaMemoryAllocator.createOnHeap(DEFAULT_ALLOCATOR_CAPACITY);
  }

  @Parameterized.Parameters(name = "inputFrameType = {0}, outputFrameType = {1}, sorted = {2}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Iterable<FrameType> inputFrameTypes = Iterables.concat(
        Collections.singletonList(null), // null means input is not a frame
        Arrays.asList(FrameType.values())
    );

    for (final FrameType inputFrameType : inputFrameTypes) {
      for (final FrameType outputFrameType : FrameType.values()) {
        for (final KeyOrder sortedness : KeyOrder.values()) {
          // Only do sortedness tests for row-based frames. (Columnar frames cannot be sorted.)
          if (sortedness == KeyOrder.NONE || outputFrameType.isRowBased()) {
            constructors.add(new Object[]{inputFrameType, outputFrameType, sortedness});
          }
        }
      }
    }

    return constructors;
  }

  @BeforeClass
  public static void setUpClass()
  {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
  }

  @Test
  public void test_string_multiValueTrue()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.TRUE);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE);
  }

  @Test
  public void test_string_multiValueFalse()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.FALSE);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE);
  }

  @Test
  public void test_string_multiValueUnknown()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.UNKNOWN);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE);
  }

  @Test
  public void test_singleValueWithEmpty_multiValueTrue()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.TRUE);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
  }

  @Test
  public void test_singleValueWithEmpty_multiValueFalse()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.FALSE);

    // When columnar frames are in multiValue = false mode, and when they see a dataset that is all single strings and
    // empty arrays, they write a single-valued column, replacing the empty arrays with nulls.
    final FrameWriterTestData.Dataset<?> expectedReadDataset =
        outputFrameType.isRowBased()
        ? FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE_WITH_EMPTY
        : FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE;

    testWithDataset(
        FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE_WITH_EMPTY,
        expectedReadDataset
    );
  }

  @Test
  public void test_singleValueWithEmpty_multiValueUnknown()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.UNKNOWN);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE_WITH_EMPTY);
  }

  @Test
  public void test_multiValueString_multiValueTrue()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.TRUE);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
  }

  @Test
  public void test_multiValueString_multiValueFalse()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.FALSE);

    if (outputFrameType.isColumnar()) {
      final IllegalStateException e = Assert.assertThrows(
          IllegalStateException.class,
          () -> testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered unexpected multi-value row"))
      );
    } else {
      testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
    }
  }

  @Test
  public void test_multiValueString_multiValueUnknown()
  {
    capabilitiesAdjustFn = capabilities -> capabilities.setHasMultipleValues(ColumnCapabilities.Capable.UNKNOWN);
    testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
  }

  @Test
  public void test_arrayString()
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_STRING);
  }

  @Test
  public void test_long()
  {
    testWithDataset(FrameWriterTestData.TEST_LONGS);
  }

  @Test
  public void test_arrayLong()
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_LONG);
  }

  @Test
  public void test_arrayFloat()
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_FLOAT);
  }

  @Test
  public void test_arrayDouble()
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_DOUBLE);
  }

  @Test
  public void test_float()
  {
    if (outputFrameType == FrameType.ROW_BASED_V1) {
      // Bug-compatible re-sorting of the floats.
      testWithDataset(
          FrameWriterTestData.TEST_FLOATS
              .sortedCopy(
                  Comparator.nullsFirst((x, y) -> TransformUtilsTest.expectedComparison(outputFrameType, x, y))));
    } else {
      testWithDataset(FrameWriterTestData.TEST_FLOATS);
    }
  }

  @Test
  public void test_double()
  {
    if (outputFrameType == FrameType.ROW_BASED_V1) {
      // Bug-compatible re-sorting of the doubles.
      testWithDataset(
          FrameWriterTestData.TEST_DOUBLES
              .sortedCopy(
                  Comparator.nullsFirst((x, y) -> TransformUtilsTest.expectedComparison(outputFrameType, x, y))));
    } else {
      testWithDataset(FrameWriterTestData.TEST_DOUBLES);
    }
  }

  @Test
  public void test_complex_hll()
  {
    testWithDataset(FrameWriterTestData.TEST_COMPLEX_HLL);
  }

  @Test
  public void test_complex_nested()
  {
    testWithDataset(FrameWriterTestData.TEST_COMPLEX_NESTED);
  }

  @Test
  public void test_typePairs()
  {
    // Test all possible arrangements of two different types.
    for (final FrameWriterTestData.Dataset<?> dataset1 : FrameWriterTestData.DATASETS) {
      for (final FrameWriterTestData.Dataset<?> dataset2 : FrameWriterTestData.DATASETS) {
        final RowSignature signature = makeSignature(Arrays.asList(dataset1, dataset2));
        final Sequence<List<Object>> rowSequence = unsortAndMakeRows(Arrays.asList(dataset1, dataset2), 1);

        final List<String> sortColumns = new ArrayList<>();
        sortColumns.add(signature.getColumnName(0));
        sortColumns.add(signature.getColumnName(1));

        try {
          final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, sortColumns);
          Assert.assertEquals(rowSequence.toList().size(), (int) writeResult.rhs);
          verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);
        }
        catch (AssertionError e) {
          throw new AssertionError(
              StringUtils.format(
                  "Assert failed in test (%s, %s)",
                  dataset1.getType(),
                  dataset2.getType()
              ),
              e
          );
        }
        catch (Throwable e) {
          throw new RE(e, "Exception in test (%s, %s)", dataset1.getType(), dataset2.getType());
        }
      }
    }
  }

  @Test
  public void test_insufficientWriteCapacity()
  {
    // Test every possible capacity for the latest row-based frame format, up to the amount required to write all
    // items from every list.
    Assume.assumeTrue(inputFrameType == null && outputFrameType == FrameType.latestRowBased());
    final RowSignature signature = makeSignature(FrameWriterTestData.DATASETS);
    final Sequence<List<Object>> rowSequence = unsortAndMakeRows(FrameWriterTestData.DATASETS, 3);
    final int totalRows = rowSequence.toList().size();

    final List<String> sortColumns = new ArrayList<>();
    for (int i = 0; i < signature.size(); i++) {
      sortColumns.add(signature.getColumnName(i));
    }

    final ByteBuffer allocatorMemory = ByteBuffer.wrap(new byte[DEFAULT_ALLOCATOR_CAPACITY]);

    boolean didWritePartial = false;
    int allocatorSize = 0;

    Pair<Frame, Integer> writeResult;

    do {
      allocatorMemory.limit(allocatorSize);
      allocatorMemory.position(0);
      allocator = ArenaMemoryAllocator.create(allocatorMemory);

      try {
        writeResult = writeFrame(rowSequence, signature, sortColumns);

        final int rowsWritten = writeResult.rhs;

        if (writeResult.rhs > 0 && writeResult.rhs < totalRows) {
          didWritePartial = true;

          verifyFrame(
              sortIfNeeded(rowSequence.limit(rowsWritten), signature, sortColumns),
              writeResult.lhs,
              signature
          );
        }
      }
      catch (Throwable e) {
        throw new RE(e, "Exception while writing with allocatorSize = %s", allocatorSize);
      }

      allocatorSize++;
    } while (writeResult.rhs != totalRows);

    verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);

    // We expect that at some point in this test, a partial frame would have been written. If not: that's strange
    // and may mean the test isn't testing the right thing.
    Assert.assertTrue("did write a partial frame", didWritePartial);
  }

  /**
   * Verifies that a frame has a certain set of expected rows. The set of expected rows will be reordered according
   * to the current {@link #sortedness} parameter.
   */
  private void verifyFrame(
      final Sequence<List<Object>> expectedRows,
      final Frame frame,
      final RowSignature signature
  )
  {
    final CursorFactory cursorFactory = FrameReader.create(signature).makeCursorFactory(frame);

    FrameTestUtil.assertRowsEqual(
        expectedRows,
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory, signature, false)
    );
  }

  /**
   * Sort output rows according to the current {@link #sortedness} and {@link #outputFrameType} parameters.
   */
  private Sequence<List<Object>> sortIfNeeded(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumnNames
  )
  {
    final List<KeyColumn> keyColumns = computeSortColumns(sortColumnNames);

    if (keyColumns.isEmpty()) {
      return rows;
    }

    final RowSignature keySignature = KeyTestUtils.createKeySignature(keyColumns, signature);
    final Comparator<RowKey> keyComparator = RowKeyComparator.create(keyColumns, signature);

    return Sequences.sort(
        rows,
        Comparator.comparing(
            row -> KeyTestUtils.createKey(keySignature, outputFrameType, row.toArray()),
            keyComparator
        )
    );
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private Pair<Frame, Integer> writeFrame(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumns
  )
  {
    return writeFrame(
        inputFrameType,
        outputFrameType,
        allocator,
        capabilitiesAdjustFn,
        rows,
        signature,
        computeSortColumns(sortColumns)
    );
  }

  /**
   * Converts the provided column names into {@link KeyColumn} according to the current {@link #sortedness}
   * parameter.
   */
  private List<KeyColumn> computeSortColumns(final List<String> sortColumnNames)
  {
    if (sortedness == KeyOrder.NONE) {
      return Collections.emptyList();
    } else {
      return sortColumnNames.stream()
                            .map(columnName -> new KeyColumn(columnName, sortedness))
                            .collect(Collectors.toList());
    }
  }

  private <T> void testWithDataset(final FrameWriterTestData.Dataset<T> dataset)
  {
    final List<T> data = dataset.getData(KeyOrder.NONE);
    final RowSignature signature = RowSignature.builder().add("x", dataset.getType()).build();
    final Sequence<List<Object>> rowSequence = rows(data);
    final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, signature.getColumnNames());

    Assert.assertEquals(data.size(), (int) writeResult.rhs);
    verifyFrame(rows(dataset.getData(sortedness)), writeResult.lhs, signature);
  }

  private <T1, T2> void testWithDataset(
      final FrameWriterTestData.Dataset<T1> writeDataset,
      final FrameWriterTestData.Dataset<T2> readDataset
  )
  {
    final List<T1> data = writeDataset.getData(KeyOrder.NONE);
    final RowSignature signature = RowSignature.builder().add("x", writeDataset.getType()).build();
    final Sequence<List<Object>> rowSequence = rows(data);
    final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, signature.getColumnNames());

    Assert.assertEquals(data.size(), (int) writeResult.rhs);
    verifyFrame(rows(readDataset.getData(sortedness)), writeResult.lhs, signature);
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private static Pair<Frame, Integer> writeFrame(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final MemoryAllocator allocator,
      @Nullable final Consumer<ColumnCapabilitiesImpl> capabilitiesAdjustFn,
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<KeyColumn> keyColumns
  )
  {
    final Segment inputSegment;

    if (inputFrameType == null) {
      // inputFrameType null means input is not a frame
      inputSegment = new RowBasedSegment<>(
          rows,
          columnName -> {
            final int columnNumber = signature.indexOf(columnName);
            return row -> columnNumber >= 0 ? row.get(columnNumber) : null;
          },
          signature
      );
    } else {
      final Frame inputFrame = writeFrame(
          null,
          inputFrameType,
          HeapMemoryAllocator.unlimited(),
          null,
          rows,
          signature,
          Collections.emptyList()
      ).lhs;

      inputSegment = new FrameSegment(inputFrame, FrameReader.create(signature));
    }

    try (final CursorHolder cursorHolder = Objects.requireNonNull(inputSegment.as(CursorFactory.class))
                                                  .makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();

      int numRows = 0;
      final FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
          outputFrameType,
          new SingleMemoryAllocatorFactory(allocator),
          signature,
          keyColumns,
          false
      );

      ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

      if (capabilitiesAdjustFn != null) {
        columnSelectorFactory = new OverrideCapabilitiesColumnSelectorFactory(
            columnSelectorFactory,
            capabilitiesAdjustFn
        );
      }

      try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactory)) {
        while (!cursor.isDone() && frameWriter.addSelection()) {
          numRows++;
          cursor.advance();
        }

        return Pair.of(Frame.wrap(frameWriter.toByteArray()), numRows);
      }
    }
  }

  /**
   * Create a row signature out of columnar lists of values.
   */
  private static RowSignature makeSignature(final List<FrameWriterTestData.Dataset<?>> datasets)
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (int i = 0; i < datasets.size(); i++) {
      final FrameWriterTestData.Dataset<?> dataset = datasets.get(i);
      signatureBuilder.add(StringUtils.format("col%03d", i), dataset.getType());
    }

    return signatureBuilder.build();
  }

  /**
   * Create rows out of shuffled (unsorted) datasets.
   */
  private static Sequence<List<Object>> unsortAndMakeRows(
      final List<FrameWriterTestData.Dataset<?>> datasets,
      final int multiplicationFactor
  )
  {
    final List<List<Object>> retVal = new ArrayList<>();

    final int rowSize = datasets.size();
    final List<Iterator<?>> iterators =
        datasets.stream()
                .map(dataset -> dataset.getData(KeyOrder.NONE).iterator())
                .collect(Collectors.toList());

    while (iterators.stream().anyMatch(Iterator::hasNext)) {
      final List<Object> row = new ArrayList<>(rowSize);

      for (int i = 0; i < rowSize; i++) {
        if (iterators.get(i).hasNext()) {
          row.add(iterators.get(i).next());
        } else {
          row.add(null);
        }
      }

      retVal.add(row);
    }

    List<List<Object>> multipliedRetVal = new ArrayList<>();
    for (int i = 0; i < multiplicationFactor; ++i) {
      multipliedRetVal.addAll(retVal);
    }

    return Sequences.simple(multipliedRetVal);
  }

  /**
   * Create a sequence of rows from a list of values. Each value appears in its own row.
   */
  private static Sequence<List<Object>> rows(final List<?> vals)
  {
    final List<List<Object>> retVal = new ArrayList<>();

    for (final Object val : vals) {
      retVal.add(Collections.singletonList(val));
    }

    return Sequences.simple(retVal);
  }

  private static class OverrideCapabilitiesColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ColumnSelectorFactory delegate;
    private final Consumer<ColumnCapabilitiesImpl> fn;

    public OverrideCapabilitiesColumnSelectorFactory(
        final ColumnSelectorFactory delegate,
        final Consumer<ColumnCapabilitiesImpl> fn
    )
    {
      this.delegate = delegate;
      this.fn = fn;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      return delegate.makeColumnValueSelector(columnName);
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      final ColumnCapabilities capabilities = delegate.getColumnCapabilities(column);
      if (capabilities == null) {
        return null;
      } else {
        final ColumnCapabilitiesImpl retVal = ColumnCapabilitiesImpl.copyOf(capabilities);
        fn.accept(retVal);
        return retVal;
      }
    }

    @Nullable
    @Override
    public RowIdSupplier getRowIdSupplier()
    {
      return delegate.getRowIdSupplier();
    }
  }
}
