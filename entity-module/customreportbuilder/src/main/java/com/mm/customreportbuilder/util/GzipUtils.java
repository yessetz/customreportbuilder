package com.mm.customreportbuilder.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class GzipUtils {
    private GzipUtils() {}

    public static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(input);
        }
        return baos.toByteArray();
    }

    public static byte[] ungzip(byte[] input) throws IOException {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(input));
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int read;
            while ((read = gis.read(buf)) != -1) {
                baos.write(buf, 0, read);
            }
            return baos.toByteArray();
        }
    }
}