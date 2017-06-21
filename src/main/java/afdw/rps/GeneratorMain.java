package afdw.rps;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

public class GeneratorMain {
    private static final long COUNT = 1024 * 1024;
    private static final long KEYS_COUNT = 1024 * 512;

    public static void main(String[] args) throws IOException {
        Files.deleteIfExists(Helpers.RAW_FILE.toPath());
        Files.deleteIfExists(Helpers.KEYS_FILE.toPath());
        try (RandomAccessFile rawRaf = new RandomAccessFile(Helpers.RAW_FILE, "rw");
             RandomAccessFile keysRaf = new RandomAccessFile(Helpers.KEYS_FILE, "rw")) {
            Helpers.writeLong(rawRaf, COUNT);
            Helpers.writeLong(keysRaf, KEYS_COUNT);
            Helpers.progress(
                COUNT,
                i -> {
                    String key = Helpers.randomString(16);
                    String value = Helpers.randomString(2048);
                    if (i < KEYS_COUNT) {
                        Helpers.writeString(keysRaf, key);
                    }
                    Helpers.writeString(rawRaf, key);
                    Helpers.writeString(rawRaf, value);
                }
            );
        }
    }
}
