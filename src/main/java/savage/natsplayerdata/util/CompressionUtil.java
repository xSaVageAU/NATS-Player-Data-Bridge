package savage.natsplayerdata.util;

import com.github.luben.zstd.Zstd;

/**
 * Utility for handling Zstandard compression.
 */
public class CompressionUtil {

    /**
     * Compresses data using Zstd (Level 1 for maximum speed).
     */
    public static byte[] compress(byte[] data) {
        return Zstd.compress(data, 1);
    }

    /**
     * Decompresses Zstd data.
     */
    public static byte[] decompress(byte[] compressedData) {
        long decompressedSize = Zstd.getFrameContentSize(compressedData);
        if (decompressedSize < 0) {
            throw new RuntimeException("§cFailed to determine decompressed size for binary data.");
        }
        return Zstd.decompress(compressedData, (int) decompressedSize);
    }
}
