package cn.rapidtsdb.tsdb.hbase246.buffer;

import java.io.IOException;

public class DynamicByteBuffer {

    private int pos;
    private byte[] buf;
    private int limitSize;


    public DynamicByteBuffer(int initialSize, int limitSize) {
        this.pos = 0;
        this.limitSize = limitSize;
        if (initialSize <= 0) {
//            throw new IllegalArgumentException(I18n.err(I18n.ERR_04354, new Object[0]));
        } else {
            this.buf = new byte[initialSize];
        }
    }

    public final void clear() {
        this.pos = 0;
    }

    public final int position() {
        return this.pos;
    }

    public final int capacity() {
        return this.buf.length;
    }

    public final byte get(int i) {
        return this.buf[i];
    }

    public final byte[] buffer() {
        return this.buf;
    }

    public final byte[] copyOfUsedBytes() {
        byte[] copy = new byte[this.pos];
        System.arraycopy(this.buf, 0, copy, 0, this.pos);
        return copy;
    }

    public final void append(byte[] bytes) throws IOException {
        if (this.pos + bytes.length > this.buf.length) {
            this.growBuffer();
        }

        System.arraycopy(bytes, 0, this.buf, this.pos, bytes.length);
        this.pos += bytes.length;
    }

    public final void append(byte b) throws IOException {
        if (this.pos >= this.buf.length) {
            this.growBuffer();
        }

        this.buf[this.pos] = b;
        ++this.pos;
    }

    public final void append(int val) throws IOException {
        if (this.pos >= this.buf.length) {
            this.growBuffer();
        }

        this.buf[this.pos] = (byte) val;
        ++this.pos;
    }

    private void growBuffer() throws IOException {
        byte[] copy;
        if (this.buf.length < limitSize) {
            copy = new byte[this.buf.length * 2];
            System.arraycopy(this.buf, 0, copy, 0, this.pos);
            this.buf = copy;
        } else {
            throw new IOException("Maximum Size Reached");
        }

    }

}
