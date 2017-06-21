package afdw.rps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.SplittableRandom;

@SuppressWarnings({"WeakerAccess", "unused", "SameParameterValue"})
public class Helpers {
    private static final char[] SYMBOLS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final SplittableRandom RANDOM = new SplittableRandom();

    public static final File RAW_FILE = new File("data.raw");
    public static final File KEYS_FILE = new File("data.keys");
    public static final File POINTERS_FILE = new File("data.pointers");
    public static final File DATA_FILE = new File("data.data");

    public static void progress(long count, IOConsumer<Long> doWork) throws IOException {
        long s = System.currentTimeMillis();
        long t = System.currentTimeMillis();
        OptionalLong checkInterval = OptionalLong.empty();
        for (long i = 0; i < count; i++) {
            doWork.accept(i);
            if ((!checkInterval.isPresent() || (i % checkInterval.getAsLong() == 0)) &&
                System.currentTimeMillis() - t >= 1000) {
                System.out.printf(
                    "Progress: %.02f; ETA: %.0f s\n",
                    (double) i / count,
                    (System.currentTimeMillis() - s) * (count / (double) i - 1) / 1000
                );
                t = System.currentTimeMillis();
                if (!checkInterval.isPresent()) {
                    checkInterval = OptionalLong.of(Math.max(1, i / 10));
                }
            }
        }
        System.out.println("Done");
    }

    public static String randomString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append(SYMBOLS[RANDOM.nextInt(SYMBOLS.length)]);
        }
        return stringBuilder.toString();
    }

    private static byte[] readBytes(DataInput dataInput, int count) throws IOException {
        byte[] bytes = new byte[count];
        dataInput.readFully(bytes);
        return bytes;
    }

    private static byte[] readBytes(ByteBuffer buffer, int count) throws IOException {
        byte[] bytes = new byte[count];
        buffer.get(bytes);
        return bytes;
    }

    public static void writeInt(DataOutput dataOutput, int number) throws IOException {
        dataOutput.write(new byte[] {
            (byte) (number >> 24),
            (byte) (number >> 16),
            (byte) (number >> 8),
            (byte) (number)
        });
    }

    public static int readInt(DataInput dataInput) throws IOException {
        byte[] read = readBytes(dataInput, 4);
        return (read[0] & 0xFF) << 24 |
            (read[1] & 0xFF) << 16 |
            (read[2] & 0xFF) << 8 |
            (read[3] & 0xFF);
    }

    public static void writeLong(DataOutput dataOutput, long number) throws IOException {
        dataOutput.write(new byte[] {
            (byte) (number >> 56),
            (byte) (number >> 48),
            (byte) (number >> 40),
            (byte) (number >> 32),
            (byte) (number >> 24),
            (byte) (number >> 16),
            (byte) (number >> 8),
            (byte) (number)
        });
    }

    public static void writeLong(ByteBuffer buffer, long number) throws IOException {
        buffer.put(new byte[] {
            (byte) (number >> 56),
            (byte) (number >> 48),
            (byte) (number >> 40),
            (byte) (number >> 32),
            (byte) (number >> 24),
            (byte) (number >> 16),
            (byte) (number >> 8),
            (byte) (number)
        });
    }

    public static long readLong(DataInput dataInput) throws IOException {
        byte[] read = readBytes(dataInput, 8);
        return (long) (read[0] & 0xFF) << 56 |
            (long) (read[1] & 0xFF) << 48 |
            (long) (read[2] & 0xFF) << 40 |
            (long) (read[3] & 0xFF) << 32 |
            (long) (read[4] & 0xFF) << 24 |
            (long) (read[5] & 0xFF) << 16 |
            (long) (read[6] & 0xFF) << 8 |
            (long) (read[7] & 0xFF);
    }

    public static long readLong(ByteBuffer buffer) throws IOException {
        byte[] read = readBytes(buffer, 8);
        return (long) (read[0] & 0xFF) << 56 |
            (long) (read[1] & 0xFF) << 48 |
            (long) (read[2] & 0xFF) << 40 |
            (long) (read[3] & 0xFF) << 32 |
            (long) (read[4] & 0xFF) << 24 |
            (long) (read[5] & 0xFF) << 16 |
            (long) (read[6] & 0xFF) << 8 |
            (long) (read[7] & 0xFF);
    }

    public static void writeString(DataOutput dataOutput, String string) throws IOException {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        writeInt(dataOutput, bytes.length);
        dataOutput.write(bytes);
    }

    public static String readString(DataInput dataInput) throws IOException {
        return new String(readBytes(dataInput, readInt(dataInput)), StandardCharsets.UTF_8);
    }

    static long getPos(long arraySize, Object key) {
        return (Integer.toUnsignedLong(key.hashCode()) % arraySize + 1) * Long.BYTES;
    }

    @FunctionalInterface
    public interface IOConsumer<T> {
        void accept(T t) throws IOException;
    }
}
