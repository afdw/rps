package afdw.rps;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

public class TransformerMain {
    public static void main(String[] args) throws IOException {
        Files.deleteIfExists(Helpers.POINTERS_FILE.toPath());
        Files.deleteIfExists(Helpers.DATA_FILE.toPath());
        try (RandomAccessFile rawRaf = new RandomAccessFile(Helpers.RAW_FILE, "r");
             RandomAccessFile pointersRaf = new RandomAccessFile(Helpers.POINTERS_FILE, "rw");
             RandomAccessFile dataRaf = new RandomAccessFile(Helpers.DATA_FILE, "rw")) {
            long count = Helpers.readLong(rawRaf);
            // noinspection UnnecessaryLocalVariable
            long arraySize = count;
            pointersRaf.writeLong(arraySize);
            Helpers.progress(
                arraySize,
                i -> pointersRaf.writeLong(0)
            );
            dataRaf.seek(Long.BYTES);
            Helpers.progress(
                count,
                i -> {
                    long dataPosition = dataRaf.getFilePointer();
                    String key = Helpers.readString(rawRaf);
                    String value = Helpers.readString(rawRaf);
                    pointersRaf.seek(Helpers.getPos(arraySize, key));
                    long pointer = Helpers.readLong(pointersRaf);
                    RandomAccessFile writeRaf = pointer != 0 ? dataRaf : pointersRaf;
                    while (pointer != 0) {
                        writeRaf.seek(pointer);
                        pointer = Helpers.readLong(writeRaf);
                    }
                    writeRaf.seek(writeRaf.getFilePointer() - Long.BYTES);
                    Helpers.writeLong(writeRaf, dataPosition);
                    dataRaf.seek(dataPosition);
                    Helpers.writeLong(dataRaf, 0);
                    Helpers.writeString(dataRaf, key);
                    Helpers.writeString(dataRaf, value);
                }
            );
        }
    }

}
