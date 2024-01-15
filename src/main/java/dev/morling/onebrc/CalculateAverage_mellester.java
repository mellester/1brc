//usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA_OPTIONS --add-modules=jdk.incubator.vector
//JAVA 21+
//JAVAC_OPTIONS --add-modules jdk.incubator.vector

/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import java.util.concurrent.*;

public class CalculateAverage_mellester {

    private static final String FILE = "./measurements.txt";
    private static RandomAccessFile raf;
    private static long MAPPING_SIZE;
    private static final int OVERLAP_SIZE = 2 << 7;
    static {
        try {
            raf = new RandomAccessFile(FILE, "r");
            MAPPING_SIZE = raf.length() / Runtime.getRuntime().availableProcessors();
            MAPPING_SIZE = Math.min(MAPPING_SIZE, Integer.MAX_VALUE - OVERLAP_SIZE * 2);
            MAPPING_SIZE = Math.max(MAPPING_SIZE, OVERLAP_SIZE * 2);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static record Measurement(Worker.ByteCharSequence station, double value) {

        private Measurement(SimpleEntry<Worker.ByteCharSequence, Double> pair) {
            this(pair.getKey(), pair.getValue());
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", this.min, this.sum / this.count, this.max);
        }
    }

    static void measurementAccumulator(MeasurementAggregator a, Measurement m) {

        a.min = Math.min(a.min, m.value);
        a.max = Math.max(a.max, m.value);
        a.sum += m.value;
        a.count++;

    }

    static MeasurementAggregator measurementCombiner(MeasurementAggregator agg1, MeasurementAggregator agg2) {
        // var res = new MeasurementAggregator();
        agg1.min = Math.min(agg1.min, agg2.min);
        agg1.max = Math.max(agg1.max, agg2.max);
        agg1.sum = agg1.sum + agg2.sum;
        agg1.count = agg1.count + agg2.count;
        return agg1;
    }

    private static record Chunk(long offset, long length) {
        public ByteBuffer buffer() {
            try {
                long length = Math.min(raf.length() - offset, this.length);
                return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, this.offset, length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String toString() {
            return "Chunk [length=" + length + ", offset=" + offset + ", end=" + (offset + length - OVERLAP_SIZE) + "]";
        }
    };

    private static final List<Chunk> mappings = new ArrayList<>();

    public static void main(String[] args) throws IOException {

        try {
            long size = raf.length();
            for (long offset = 0; offset < size; offset += MAPPING_SIZE) {
                long lenght;
                if (offset + MAPPING_SIZE > size) {
                    lenght = (int) (size - offset);
                }
                else {
                    lenght = MAPPING_SIZE + OVERLAP_SIZE;
                }
                lenght = Math.min(lenght, size - offset);

                mappings.add(new Chunk(offset, lenght));
            }
        }
        catch (IOException e) {
            raf.close();
            throw e;
        }
        System.err.println("availableProcessors " + Runtime.getRuntime().availableProcessors());
        try (ExecutorService threadExecutors = Executors
                .newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            CompletionService<Map<String, MeasurementAggregator>> completionService = new ExecutorCompletionService<>(
                    threadExecutors);
            for (int i = 0; i < mappings.size(); i++) {
                Chunk record = mappings.get(i);
                completionService.submit(new Worker(i, record));
            }
            int received = 0;
            boolean errors = false;
            var treeMap = new TreeMap<String, MeasurementAggregator>();
            while (received < mappings.size() && !errors) {
                Future<Map<String, MeasurementAggregator>> resultFuture = completionService.take();
                try {
                    Map<String, MeasurementAggregator> resultChunk = resultFuture.get();
                    received++;
                    if (resultChunk != null) {
                        resultChunk.forEach((k, v) -> {
                            treeMap.merge(k, v, CalculateAverage_mellester::measurementCombiner);
                        });
                    }
                }
                catch (Exception e) {
                    System.err.println("Error getting result out of future");
                    e.printStackTrace();
                    errors = true;
                }
                resultFuture = null;
            }

            threadExecutors.shutdown();

            // if (System.getProperty("log.enable") == null)
            System.out.println(treeMap);
        }
        catch (InterruptedException e) {

            e.printStackTrace();
            System.exit(1);

        }
        System.exit(0);

    }

    private static class Worker implements Callable<Map<String, MeasurementAggregator>> {

        private final int id;
        private final Chunk chunk;
        private final Map<ByteCharSequence, MeasurementAggregator> result = new HashMap<>();

        public Worker(int id, Chunk chunk) {
            this.id = id;
            this.chunk = chunk;
        }

        void log(Object arg) {
            if (System.getProperty("log.enable") != null) {
                System.out.println("[" + id + "]" + arg);
            }
        }

        private int old_index = 0;
        private static final byte SEPERATOR1 = (byte) '\n';
        private static final byte SEPERATOR2 = (byte) ';';
        private ByteBuffer buffer = null;
        private byte[] bytes = null;

        @Override
        public Map<String, MeasurementAggregator> call() {
            try {

                buffer = chunk.buffer();
                if (this.id != 0)
                    skipToNextLine();
                int remaining = buffer.remaining();
                int buffer_size = (int) Math.min(remaining, OVERLAP_SIZE * 2);
                this.bytes = new byte[buffer_size];
                int offset = 0;
                int bytesToRead = buffer_size;
                while (bytesToRead > 0) {
                    buffer.get(bytes, offset, bytesToRead);
                    remaining -= bytesToRead;
                    Arrays.fill(bytes, offset + bytesToRead, bytes.length, (byte) 0);
                    int parsed = parse();
                    System.arraycopy(bytes, parsed, bytes, 0, bytes.length - parsed);
                    offset = bytes.length - parsed;
                    bytesToRead = Math.min(buffer_size - offset, remaining);
                    old_index = 0;

                }
                TreeMap<String, MeasurementAggregator> toRetrun = new TreeMap<>();
                for (var entry : result.entrySet()) {
                    var s = entry.getKey().toString();
                    toRetrun.put(s.intern(), entry.getValue());
                }
                return toRetrun;

            }
            catch (Exception e) {
                // TODO: handle exception
                return null;
            }
        }

        private boolean skipToNextLine() {
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == SEPERATOR1) {
                    return true;
                }
            }
            return false;
        }

        private int parse() {
            final int[] seperatorIndexs = IntStream.range(0, (int) bytes.length)
                    .filter(i -> {
                        byte b = bytes[i];
                        return b == SEPERATOR1 || b == SEPERATOR2;
                    }).toArray();
            var pairs = IntStream.range(0, seperatorIndexs.length / 2)
                    .mapToObj(
                            i -> new SimpleEntry<Integer, Integer>(seperatorIndexs[i * 2], seperatorIndexs[i * 2 + 1]))
                    .map(i -> {
                        var station = new ByteCharSequence(Arrays.copyOfRange(bytes, old_index, i.getKey()));
                        var value = new ByteCharSequence(Arrays.copyOfRange(bytes, i.getKey() + 1, i.getValue()));
                        old_index = i.getValue() + 1;
                        return new SimpleEntry<ByteCharSequence, Double>(station, parseDouble(value));
                    }).toList();
            int toReturn = old_index;

            Collector<Measurement, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
                    MeasurementAggregator::new,
                    CalculateAverage_mellester::measurementAccumulator,
                    CalculateAverage_mellester::measurementCombiner,
                    agg -> {
                        return agg;
                    },
                    Collector.Characteristics.UNORDERED, Collector.Characteristics.CONCURRENT);

            var a = pairs.stream().parallel()
                    .map(l -> new Measurement(l)).toList();
            var b = a.stream().collect(groupingByConcurrent(m -> m.station(), collector));
            b.forEach((
                       station, measurementAggregator) -> result.merge(station, measurementAggregator,
                               CalculateAverage_mellester::measurementCombiner));

            return toReturn;
        }

        // Byte view into Worker.this.bytes
        private class ByteCharSequence implements CharSequence, Comparable<ByteCharSequence> {

            private byte[] bytes = null;

            public ByteCharSequence(byte[] bytes) {
                this.bytes = bytes;
                // assert (bytes.length >= 2);
            }

            @Override
            public int length() {
                return this.bytes.length;
            }

            @Override
            public char charAt(int index) {
                return (char) this.bytes[index];
            }

            @Override
            public ByteCharSequence subSequence(int start, int end) {
                return new ByteCharSequence(Arrays.copyOfRange(this.bytes, start, end));
            }

            @Override
            public int compareTo(ByteCharSequence arg0) {
                return Arrays.compare(this.bytes, arg0.bytes);
            }

            @Override
            public boolean equals(Object obj) {
                var otherBytes = ((ByteCharSequence) obj).bytes;
                return Arrays.equals(this.bytes, otherBytes);
            }

            @Override
            public int hashCode() {
                // return Arrays.hashCode(this.bytes);
                byte byte1 = this.bytes[0];
                byte byte2 = this.bytes[this.bytes.length - 1];
                return ((byte1 & 0xFF) << 8) | (byte2 & 0xFF);
            }

            @Override
            public String toString() {
                return new String(this.bytes);
            }

        }
    }

    // https://stackoverflow.com/a/75470082
    public static double parseDouble(CharSequence chars) {
        long left = 0;
        long right = 0;
        long numRight = 0;
        int negateFactor = 1;
        boolean isDecimal = false;
        for (int i = 0; i < chars.length(); i++) {
            char ch = chars.charAt(i);
            if (ch == '-') {
                negateFactor = -1;
                continue;
            }

            if (ch == '.') {
                isDecimal = true;
                continue;
            }

            int digit = Character.getNumericValue(ch);
            if (!isDecimal) {
                if (left == 0) {
                    left = digit;
                }
                else {
                    left *= 10;
                    left += digit;
                }
            }
            else {
                numRight++;
                if (numRight >= 9)
                    break;
                if (right == 0) {
                    right = digit;
                }
                else {
                    right *= 10;
                    right += digit;
                }
            }
        }

        double decimal = left + right * Math.pow(10, -numRight);
        decimal *= negateFactor;

        return decimal;
    }

}