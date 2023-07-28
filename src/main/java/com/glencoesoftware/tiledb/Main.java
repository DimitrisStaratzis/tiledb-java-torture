/*
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.glencoesoftware.tiledb;

import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;

import java.awt.Rectangle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.Filter;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryStatus;
import io.tiledb.java.api.SubArray;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.ZstdFilter;
import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;

public class Main implements AutoCloseable {

    /** Tile size in the X dimension */
    private final int tileSizeX = 1000;

    /** Tile size in the Y dimension */
    private final int tileSizeY = 1000;

    /** Number of timepoints */
    private final int sizeT = 1;

    /** Number of channels */
    private final int sizeC = 20;

    /** Number of sections/slices in the axial plane */
    private final int sizeZ = 1;

    /** Height of the image */
    private final int sizeY = 22000;

    /** Width of the image */
    private final int sizeX = 13000;

    /**
     * TileDB array data type which reflects an unsigned 16-bit integer pixel
     * type
     */
    private final Datatype attrType = Datatype.TILEDB_UINT16;

    /** Number of bytes in memory that a single pixel (TileDB cell) occupies */
    private final int bytesPerPixel;

    /** Whether the data is of 3 channel unsigned 8-bit integer pixel type */
    private final boolean rgb = false;

    /** Root where the TileDB arrays will be created, one per resolution */
    private final Path tileDbRoot;

    /** Java string vs. string representation of the desired TileDB config */
    private final Map<String, String> tileDbConfig;

    /**
     * Number of parallel workers that will be reading and writing from the
     * various TileDB arrays.
     */
    private final int maxWorkers = 4;

    /**
     * Fixed thread pool executor of {@link maxWorkers} size where tile read
     * and write operations will be performed.
     */
    private final ExecutorService executor;

    /** Number of resolutions we will process */
    private final int resolutions = 2;

    /**
     * Tile to tile random overlap bounds which will used to simulate real
     * world non-adjacent writes.
     */
    private final int overlap = 100;

    public static void main(String[] args) throws Exception {
        try (Main main = new Main()) {
            main.calculateResolutionZero();
            main.calculatePyramid();
        } catch (Exception e) {
            System.err.println("Exception during execution");
            e.printStackTrace();
        }
    }

    public Main() throws Exception {
        bytesPerPixel = (int) attrType.size();
        tileDbRoot = Files.createTempDirectory(
            Paths.get("").toAbsolutePath(),  // Current working directory
            "tiledb_"
        );
        System.out.println("TileDB root is: " + tileDbRoot);

        // Don't consolidate
        tileDbConfig = Map.of();
        /*
        // Consolidate
        tileDbConfig = Map.of(
                "sm.consolidate.mode", "fragment_meta",
                "sm.consolidate.step_min_frag", "0"
        );
        */

        executor = Executors.newFixedThreadPool(this.maxWorkers);
    }

    private String _toString(SubArray subarray) throws TileDBError {
        // Assume 5 dimensions
        List<String> slices = new ArrayList<String>();
        for (int dimIdx = 0; dimIdx < 5; dimIdx++) {
            assert subarray.getRangeNum(dimIdx) == 1;
            Pair<Object, Object> range = subarray.getRange(dimIdx, 0);
            slices.add(String.join(":",
                    range.getFirst().toString(),
                    range.getSecond().toString()
                ));
        }
        return String.format("[%s]", String.join(", ",
                slices.toArray(new String[slices.size()])
            ));
    }

    private String createArray(
            int resolution, int extentY, int extentX)
                    throws TileDBError {
        try (Context ctx = new Context()) {
            // Dimension AutoClosable native memory cleanup will be performed by
            // the closure of the domain below.
            Dimension<Integer> t = new Dimension<Integer>(
                    ctx, "t", Integer.class,
                    new Pair<Integer, Integer>(0, sizeT - 1), 1);
            Dimension<Integer> c = new Dimension<Integer>(
                    ctx, "c", Integer.class,
                    new Pair<Integer, Integer>(0, sizeC - 1), 1);
            Dimension<Integer> z = new Dimension<Integer>(
                    ctx, "z", Integer.class,
                    new Pair<Integer, Integer>(0, sizeZ - 1), 1);
            Dimension<Integer> y = new Dimension<Integer>(
                    ctx, "y", Integer.class,
                    new Pair<Integer, Integer>(0, sizeY - 1), extentY);
            Dimension<Integer> x = new Dimension<Integer>(
                    ctx, "x", Integer.class,
                    new Pair<Integer, Integer>(0, sizeX - 1), extentX);

            try (Domain domain = new Domain(ctx);
                 Attribute a1 = new Attribute(ctx, "a1", attrType);
                 FilterList filterList = new FilterList(ctx);
                 Filter filter = new ZstdFilter(ctx);
                 ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE)) {
                domain.addDimension(t);
                domain.addDimension(c);
                domain.addDimension(z);
                domain.addDimension(y);
                domain.addDimension(x);

                filterList.addFilter(filter);
                a1.setFilterList(filterList);
                if (rgb) {
                    a1.setFillValue((short) 0xff);
                } else {
                    switch (attrType) {
                        case TILEDB_INT8:
                            a1.setFillValue((byte) 0);
                            break;
                        case TILEDB_UINT8:
                        case TILEDB_INT16:
                            a1.setFillValue((short) 0);
                            break;
                        case TILEDB_UINT16:
                            a1.setFillValue((int) 0);
                            break;
                        case TILEDB_FLOAT32:
                            a1.setFillValue((float) 0);
                            break;
                        case TILEDB_FLOAT64:
                            a1.setFillValue((double) 0);
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported pixel type: "
                                    + attrType);
                    }
                }

                schema.setDomain(domain);
                schema.addAttribute(a1);

                String path = tileDbRoot
                        .resolve(Integer.toString(resolution))
                        .toString();
                System.out.println("Creating TileDB array: " + path);
                Array.create(path, schema);
                return path;
            }
        }
    }

    private Config createConfig() throws TileDBError {
        Config config = new Config();
        for (Entry<String, String> entry : tileDbConfig.entrySet()) {
            config.set(entry.getKey(),  entry.getValue());
        }
        return config;
    }

    private void writeImage(
            final Context ctx, final Array array,
            final int t, final int c, final int z, final int y0, final int x0)
                    throws TileDBError, InterruptedException {
        int y1 = Math.min(y0 + tileSizeY - 1, sizeY - 1);
        int x1 = Math.min(x0 + tileSizeX - 1, sizeX - 1);
        int area = ((y1 + 1) - y0) * ((x1 + 1) - x0);
        ByteBuffer asByteBuffer = ByteBuffer
            .allocateDirect(area * bytesPerPixel)
            .order(ByteOrder.nativeOrder());
        try (Query query = new Query(array, TILEDB_WRITE);
             SubArray subarray = new SubArray(ctx, array)) {
            query.setLayout(TILEDB_ROW_MAJOR);
            subarray.addRange(0, t, t, null);
            subarray.addRange(1, c, c, null);
            subarray.addRange(2, z, z, null);
            subarray.addRange(3, y0, y1, null);
            subarray.addRange(4, x0, x1, null);
            query.setSubarray(subarray);
            query.setDataBuffer("a1", asByteBuffer);
            QueryStatus status = query.submit();
            System.out.println(String.format(
                    "Inserted rectangle: %s; status: %s",
                    _toString(subarray), status));
        }
    }

    private void calculateResolutionZero(
            final Context ctx, final Array array,
            final int t, final int c, final int z)
                    throws InterruptedException, ExecutionException,
                            TileDBError {
        List<CompletableFuture<Void>> futures =
                new ArrayList<CompletableFuture<Void>>();
        int gridSizeY = (int) Math.ceil((double) sizeY / tileSizeY);
        int gridSizeX = (int) Math.ceil((double) sizeX / tileSizeX);
        for (int y = 0; y < gridSizeY; y++) {
            for (int x = 0; x < gridSizeX; x++) {
                final int tileY = y * tileSizeY + new Random().nextInt(overlap) + 1;
                final int tileX = x * tileSizeX + new Random().nextInt(overlap) + 1;
                CompletableFuture<Void> future =
                        new CompletableFuture<Void>();
                executor.execute(() -> {
                    try {
                        writeImage(ctx, array, t, c, z, tileY, tileX);
                        future.complete(null);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
                futures.add(future);
            }
            // Wait until the entire channel-row has completed before
            // proceeding to the next one
            CompletableFuture.allOf(futures.toArray(
                    new CompletableFuture[futures.size()])).join();
        }
    }

    private void consolidate(String uri) throws TileDBError {
        if (tileDbConfig.keySet().stream()
                .noneMatch(v -> v.startsWith("sm.consolidate"))) {
            System.out.println("Not consolidating " + uri);
            return;
        }
        System.out.println(
                "Consolidating " + uri + " based on TileDB configuration");
        try (Config config = createConfig();
             Context ctx = new Context(config)) {
            Array.consolidate(ctx, uri, config);
            Array.vacuum(ctx, uri);
        }
    }

    void calculateResolutionZero()
            throws InterruptedException, ExecutionException,
                IOException, TileDBError {
        String uri = createArray(0, tileSizeY, tileSizeX);
        try (Config config = createConfig();
             Context ctx = new Context(config)) {
            try (Array array = new Array(ctx, uri, TILEDB_WRITE)) {
                for (int t = 0; t < sizeT; t++) {
                    for (int c = 0; c < sizeC; c++) {
                        for (int z = 0; z < sizeZ; z++) {
                            System.out.println(String.format(
                                "Calculate resolution zero for T:%d C:%d Z:%d",
                                t, c, z));
                            calculateResolutionZero(ctx, array, t, c, z);
                        }
                    }
                }
            }
        }
        consolidate(uri);
    }

    private ByteBuffer downsampleTileSIS(ByteBuffer source, Rectangle size)
            throws TileDBError {
        IImageScaler scaler = new SimpleImageScaler();
        boolean isLittleEndian = true;  // We're on the JVM
        boolean isFloatingPoint = !attrType.isIntegerType();
        int area = size.width * size.height;
        byte[] sourceArray = new byte[(int) area * bytesPerPixel];
        source.get(sourceArray, 0, sourceArray.length);
        byte[] destinationArray = scaler.downsample(
                sourceArray, (int) size.width, (int) size.height,
                2, bytesPerPixel, isLittleEndian, isFloatingPoint,
                1, false);
        source.clear().position(source.capacity() - destinationArray.length);
        ByteBuffer destination = source.slice().order(ByteOrder.nativeOrder());
        source.put(destinationArray);
        return destination;
    }

    private void processTile(
            final Context ctx, final Array source, final Array destination,
            final int resolution, final int t, final int c, final int z,
            final int y0, final int x0)
                    throws TileDBError, InterruptedException {
        // Source offsets, pyramid scale is 2
        int factor = (int) Math.pow(2, resolution);
        int previousFactor = (int) Math.pow(2, resolution - 1);
        int sizeY = this.sizeY / factor;
        int sizeX = this.sizeX / factor;
        int sourceY0 = y0 * 2;
        int sourceY1 = Math.min(
                sourceY0 + tileSizeY * 2, this.sizeY / previousFactor) - 1;
        int sourceX0 = x0 * 2;
        int sourceX1 = Math.min(
                sourceX0 + tileSizeX * 2, this.sizeX / previousFactor) - 1;
        Rectangle sourceSize = new Rectangle(
                sourceX1 - sourceX0 + 1, sourceY1 - sourceY0 + 1);
        int y1 = Math.min(y0 + (int) sourceSize.height / 2, sizeY) - 1;
        int x1 = Math.min(x0 + (int) sourceSize.width / 2, sizeX) - 1;

        ByteBuffer sourceBuffer = ByteBuffer
                .allocateDirect(sourceSize.height * sourceSize.width * bytesPerPixel)
                .order(ByteOrder.nativeOrder());
        try (Query sourceQuery = new Query(source, TILEDB_READ);
             Query destinationQuery = new Query(destination, TILEDB_WRITE);
             SubArray sourceSubarray = new SubArray(ctx, source);
             SubArray destinationSubarray = new SubArray(ctx, destination)) {
          sourceSubarray.addRange(0, t, t, null);
          sourceSubarray.addRange(1, c, c, null);
          sourceSubarray.addRange(2, z, z, null);
          sourceSubarray.addRange(3, sourceY0, sourceY1, null);
          sourceSubarray.addRange(4, sourceX0, sourceX1, null);
          sourceQuery.setSubarray(sourceSubarray);
          sourceQuery.setDataBuffer("a1", sourceBuffer);
          QueryStatus sourceStatus = sourceQuery.submit();
          System.out.println(String.format(
                  "Read rectangle: %s; status: %s",
                  _toString(sourceSubarray), sourceStatus));

          // Destination buffer will be a correctly sized slice of the
          // original source buffer and consequently does not need to be
          // separately released
          ByteBuffer destinationBuffer =
                  downsampleTileSIS(sourceBuffer, sourceSize);

          destinationSubarray.addRange(0, t, t, null);
          destinationSubarray.addRange(1, c, c, null);
          destinationSubarray.addRange(2, z, z, null);
          destinationSubarray.addRange(3, y0, y1, null);
          destinationSubarray.addRange(4, x0, x1, null);
          destinationQuery.setSubarray(destinationSubarray);
          destinationQuery.setDataBuffer("a1", destinationBuffer);
          QueryStatus destinationStatus = destinationQuery.submit();
          System.out.println(String.format(
                  "Wrote rectangle: %s; status: %s",
                  _toString(destinationSubarray), destinationStatus));
       }
    }

    private void calculatePyramid(
            final Context ctx, final int resolution, final int t, final int c,
            final int z)
                    throws InterruptedException, ExecutionException,
                    IOException, TileDBError {
        String sourceRoot = tileDbRoot.resolve(
                Integer.toString(resolution - 1)).toString();
        String destinationRoot = tileDbRoot.resolve(
                Integer.toString(resolution)).toString();
        try (Array source = new Array(ctx, sourceRoot, TILEDB_READ);
             Array destination = new Array(ctx, destinationRoot, TILEDB_WRITE)) {
            List<CompletableFuture<Void>> futures =
                    new ArrayList<CompletableFuture<Void>>();
            int gridSizeY = (int) Math.ceil((double) sizeY / 2 / tileSizeY);
            int gridSizeX = (int) Math.ceil((double) sizeX / 2 / tileSizeX);
            for (int tileY = 0; tileY < gridSizeY; tileY++) {
                for (int tileX = 0; tileX < gridSizeX; tileX++) {
                    final int y = tileY * tileSizeY;
                    final int x = tileX * tileSizeX;
                    CompletableFuture<Void> future =
                            new CompletableFuture<Void>();
                    futures.add(future);
                    executor.execute(() -> {
                        try {
                            processTile(
                                ctx, source, destination, resolution, t, c, z, y, x);
                            future.complete(null);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    });
                }
            }
            // Wait until the entire resolution has completed before proceeding
            // to the next one
            CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[futures.size()])).join();
        }
    }

    void calculatePyramid()
            throws InterruptedException, ExecutionException, IOException,
                TileDBError {
        for (int resolution = 1; resolution < resolutions; resolution++) {
            String uri = createArray(
                    resolution, tileSizeY, tileSizeX);
            for (int t = 0; t < sizeT; t++) {
                for (int c = 0; c < sizeC; c++) {
                    try (Config config = createConfig();
                         Context ctx = new Context(config)) {
                        for (int z = 0; z < sizeZ; z++) {
                            System.out.println(String.format(
                                    "Calculate pyramid for " +
                                    "Resolution:%d T:%d C:%d Z:%d",
                                    resolution, t, c, z));
                            calculatePyramid(ctx, resolution, t, c, z);
                        }
                    }
                }
            }
            consolidate(uri);
        }
    }

    @Override
    public void close() throws Exception {
        // Shut down first, tasks may still be running
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}
