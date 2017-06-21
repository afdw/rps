package afdw.rps;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Optional;

public class ReaderMain {
    private static final boolean USE_POINTERS_CACHE = Boolean.TRUE;

    private static Optional<String> get(long arraySize,
                                        RandomAccessFile pointersRaf,
                                        ByteBuffer pointersBuffer,
                                        RandomAccessFile dataRaf,
                                        String key) throws IOException {
        if (USE_POINTERS_CACHE) {
            pointersBuffer.position((int) Helpers.getPos(arraySize, key));
        } else {
            pointersRaf.seek(Helpers.getPos(arraySize, key));
        }
        long pointer = USE_POINTERS_CACHE ? Helpers.readLong(pointersBuffer) : Helpers.readLong(pointersRaf);
        while (pointer != 0) {
            dataRaf.seek(pointer);
            pointer = Helpers.readLong(dataRaf);
            if (Helpers.readString(dataRaf).equals(key)) {
                return Optional.of(Helpers.readString(dataRaf));
            }
        }
        return Optional.empty();
    }

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile keysRaf = new RandomAccessFile(Helpers.KEYS_FILE, "r");
             RandomAccessFile pointersRaf = new RandomAccessFile(Helpers.POINTERS_FILE, "r");
             RandomAccessFile dataRaf = new RandomAccessFile(Helpers.DATA_FILE, "r")) {
            ByteBuffer pointersBuffer = null;
            if (USE_POINTERS_CACHE) {
                pointersBuffer = ByteBuffer.allocateDirect((int) pointersRaf.getChannel().size());
                pointersRaf.getChannel().read(pointersBuffer);
                pointersBuffer.rewind();
            }
            long arraySize = USE_POINTERS_CACHE ? Helpers.readLong(pointersBuffer) : Helpers.readLong(pointersRaf);
            long keysCount = Helpers.readLong(keysRaf);
            long t = System.currentTimeMillis();
            for (long i = 0; i < keysCount; i++) {
                long finalI = i;
                get(arraySize, pointersRaf, pointersBuffer, dataRaf, Helpers.readString(keysRaf))
                    .orElseThrow(() -> new RuntimeException("Can't read " + finalI));
            }
            System.out.printf("%.02f reads/s\n", (double) keysCount / (System.currentTimeMillis() - t) * 1000);
        }
    }
}
