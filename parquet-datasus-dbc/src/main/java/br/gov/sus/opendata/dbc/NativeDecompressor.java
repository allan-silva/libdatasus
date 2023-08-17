package br.gov.sus.opendata.dbc;

public class NativeDecompressor {

    static {
        System.loadLibrary("blast_middleware_rs");
    }

    public static native void decompress(String inputFile, DecompressStats decompressStats);

    public static native void decompressTo(String inputFile, String outputFile, DecompressStats decompressStats);

    public static class DecompressStats {
        private long inputFileSize;

        private String inputFileName;

        private long outputFileSize;

        private String outputFileName;

        private long decompressTime;

        private int decompressStatusCode;

        public long getInputFileSize() {
            return inputFileSize;
        }

        public void setInputFileSize(long inputFileSize) {
            this.inputFileSize = inputFileSize;
        }

        public String getInputFileName() {
            return inputFileName;
        }

        public void setInputFileName(String inputFileName) {
            this.inputFileName = inputFileName;
        }

        public long getOutputFileSize() {
            return outputFileSize;
        }

        public void setOutputFileSize(long outputFileSize) {
            this.outputFileSize = outputFileSize;
        }

        public String getOutputFileName() {
            return outputFileName;
        }

        public void setOutputFileName(String outputFileName) {
            this.outputFileName = outputFileName;
        }

        public long getDecompressTime() {
            return decompressTime;
        }

        public void setDecompressTime(long decompressTime) {
            this.decompressTime = decompressTime;
        }

        public int getDecompressStatusCode() {
            return decompressStatusCode;
        }

        public void setDecompressStatusCode(int decompressStatusCode) {
            this.decompressStatusCode = decompressStatusCode;
        }
    }
}
