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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import javax.swing.JPopupMenu.Separator;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import jdk.incubator.vector.VectorSpecies;
import jdk.javadoc.internal.doclets.formats.html.resources.standard;
import jdk.incubator.vector.*;

public class CalculateAverage_mellester {

    private static final String FILE = "./measurements.txt";
    private static RandomAccessFile raf;
    static {
        try {
            raf = new RandomAccessFile(FILE, "r");
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + FILE);
            System.exit(1);
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static record Measurement(Worker.ByteCharSequence station, double value) {

        private Measurement(Worker.ByteCharSequence[] parts) {
            this(parts[0], CalculateAverage_mellester.parseDouble(parts[1]));
        }

    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
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

    private static final int MAPPING_SIZE = 1 << 9;
    private static final long OVERLAP_SIZE = 2 << 7;
    static {
        try {
            System.out.println("RAF_lenght: " + raf.length());
        } catch (Exception e) {
            // TODO: handle exception
            System.err.println("Error getting RAF lenght");
        }
        System.out.println("MAPPING_SIZE: " + MAPPING_SIZE);
        System.out.println("OVERLAP_SIZE: " + OVERLAP_SIZE);

    }

    private static record Chunk(long offset, long length) {
        public ByteBuffer buffer() {
            try {
                return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, this.offset, this.length);
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

        final VectorSpecies<Byte> SPECIES = VectorSpecies.ofPreferred(byte.class);
        int length = SPECIES.length();
        System.out.println("SPECIES: " + SPECIES);
        System.out.println("length: " + length);
        try {
            long size = raf.length();
            for (long offset = 0; offset < size; offset += MAPPING_SIZE) {
                int lenght = (int) Math.min(size - offset, MAPPING_SIZE + OVERLAP_SIZE);
                mappings.add(new Chunk(offset, lenght));
            }
            System.out.println("mappings: " + mappings.size());
        } catch (IOException e) {
            raf.close();
            throw e;
        }

        try (ExecutorService threadExecutors = Executors.newVirtualThreadPerTaskExecutor()) {
            int count = 0;
            var result = new ArrayList<Future<Object>>(mappings.size());
            for (int i = 0; i < mappings.size(); i++) {
                Chunk record = mappings.get(i);
                if (count++ == 2) {
                    System.out.println("count: " + count);
                    break;
                }
                result.add(threadExecutors.submit(new Worker(i, record)));
            }

         

            threadExecutors.shutdown();
            boolean TerminatedWithinTime = threadExecutors.awaitTermination(2, TimeUnit.MINUTES);

            result.stream().map(f -> {
                try {
                    return (MeasurementAggregator) f.get();
                } catch (Exception e) {
                }
                return null;
            }).toList();


            Collector<MeasurementAggregator, MeasurementAggregator, ResultRow> collector = Collector.of(
                    MeasurementAggregator::new,
                    CalculateAverage_mellester::measurementCombiner,

                    CalculateAverage_mellester::measurementCombiner,
                    agg -> {
                        return new ResultRow(agg.min, agg.sum / agg.count, agg.max);
                    },
                    Collector.Characteristics.UNORDERED, Collector.Characteristics.CONCURRENT);

            if (TerminatedWithinTime) {
                System.out.println("Finished");
            } else {
                System.out.println("Not finished");
            }
        } catch (InterruptedException e) {

            e.printStackTrace();
            System.exit(1);

        }
        System.exit(0);

    }

    private static class Worker implements Callable<Object> {

        private final int id;
        private final Chunk chunk;

        public Worker(int id, Chunk chunk) {
            this.id = id;
            this.chunk = chunk;
        }

        private Integer old_index = 0;
        private static final byte SEPERATOR = (byte) '\n';
        private ByteBuffer buffer = null;
        private byte[] bytes = null;

        private long lastOcurenceOfSeperator = -1;

        private void findLastOcurenceOfSeperator() {
            if (id == 0)
                return;
            IntStream.range(0, (int) OVERLAP_SIZE).filter(i -> bytes[(int) OVERLAP_SIZE - i] == SEPERATOR).findFirst()
                    .ifPresentOrElse((i) -> {
                        lastOcurenceOfSeperator = i;
                    }, () -> {
                        System.err.println("[" + id + "] No Seprator found in overlap");
                        System.out.println(chunk.offset());
                        System.out.println(new String(Worker.this.bytes));
                        throw new RuntimeException("No Seprator found in overlap");
                    });

        }

        @Override
        public Map<String, MeasurementAggregator> call() {
            System.out.println("[" + this.id + "]" + chunk);
            final long offset = chunk.offset();
            long lenght = MAPPING_SIZE + OVERLAP_SIZE;
            try {
                lenght = Math.min(raf.length() - offset, lenght);
                buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, lenght);
            } catch (IOException e) {
                System.err.println("Error mapping file: " + offset + " " + lenght);
                throw new UncheckedIOException(e);
            }
            this.bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            findLastOcurenceOfSeperator();

            System.out.println("[" + this.id + "] lastOcurenceOfSeperator: " + lastOcurenceOfSeperator);
            final List<ByteCharSequence> lines = new ArrayList<>(60);

            System.out.println("[" + this.id + "] bytes: " + bytes.length);
            IntStream.range((int) lastOcurenceOfSeperator + 1, (int) bytes.length)
                    .filter(i -> bytes[i] == SEPERATOR).boxed()
                    .map(i -> {
                        var temp = new int[] { old_index, i - 1 };
                        old_index = i + 1;
                        return temp;
                    }).toList().stream().parallel()
                    .forEach((int[] pair) -> {
                        lines.add(new ByteCharSequence(pair[0], pair[1]));
                        // System.out.println("[" + this.id + "] index: " + pair[0] + " " + pair[1]);
                        System.out.println("[" + this.id + "] line: " + pair[0] + " " + pair[1] + "\n"
                                + new ByteCharSequence(pair[0], pair[1]));
                    });

            System.out.println("[" + this.id + "] lines: " + lines.size());
            try {

                Collector<Measurement, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
                        MeasurementAggregator::new,
                        CalculateAverage_mellester::measurementAccumulator,
                        CalculateAverage_mellester::measurementCombiner,
                        agg -> {
                            return agg;
                        },
                        Collector.Characteristics.UNORDERED, Collector.Characteristics.CONCURRENT);

                var b = lines.stream().parallel()
                        .map(l -> new Measurement(l.split((byte) ';')))
                        .collect(groupingBy(m -> m.station(), collector));
                Map<ByteCharSequence, MeasurementAggregator> measurements = new TreeMap<ByteCharSequence, MeasurementAggregator>(
                        b);

                System.out.println("[" + this.id + "] measurements: " + measurements);
                return measurements.entrySet().stream().collect(toMap(e -> e.getKey().toString(), e -> e.getValue()));
            } catch (Exception e) {
                // TODO: handle exception
                System.err.println("Error in worker: " + this.id);
                e.printStackTrace();
                return null;
            }
        }

        // Byte view into Worker.this.bytes
        private class ByteCharSequence implements CharSequence, Comparable<ByteCharSequence> {

            private final int end;
            private final int start;

            public ByteCharSequence(int start, int end) {
                this.start = start;
                this.end = end;
            }

            @Override
            public int length() {
                return this.end - this.start;
            }

            @Override
            public char charAt(int index) {
                try {

                    return (char) (Worker.this.bytes[start + index] & 0xff);
                } catch (Exception e) {
                    // TODO: handle exception
                    System.err.println("Error in charAt: " + index);
                    throw e;
                }
            }

            @Override
            public ByteCharSequence subSequence(int start, int end) {
                return new ByteCharSequence(start + start, end - start);
            }

            public ByteCharSequence[] split(Byte sepreator) {
                ByteCharSequence[] result = { this, null };
                for (int i = 0; i < length(); i++) {
                    if (charAt(i) == sepreator) {
                        result[0] = subSequence(0, i);
                        result[1] = subSequence(i + 1, end - i);
                    }
                }
                if (result[1] == null) {
                    System.err.println("No seperator found in line: " + this);
                }
                return result;
            }

            @Override
            public int compareTo(ByteCharSequence arg0) {
                return Arrays.compare(Worker.this.bytes, this.start, this.end, Worker.this.bytes, arg0.start, arg0.end);
            }

            @Override
            public String toString() {
                return new String(Worker.this.bytes, this.start, this.end - this.start + 1);
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
                } else {
                    left *= 10;
                    left += digit;
                }
            } else {
                numRight++;
                if (numRight >= 9)
                    break;
                if (right == 0) {
                    right = digit;
                } else {
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