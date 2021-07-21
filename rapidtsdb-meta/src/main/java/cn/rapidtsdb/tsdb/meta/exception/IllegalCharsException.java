package cn.rapidtsdb.tsdb.meta.exception;

public class IllegalCharsException extends Exception {
    char illegalChar;

    public IllegalCharsException(char illegalChar) {
        this.illegalChar = illegalChar;
    }

    public IllegalCharsException(String message, char illegalChar) {
        super(message);
        this.illegalChar = illegalChar;
    }

    public IllegalCharsException(String message, Throwable cause, char illegalChar) {
        super(message, cause);
        this.illegalChar = illegalChar;
    }

    public IllegalCharsException(Throwable cause, char illegalChar) {
        super(cause);
        this.illegalChar = illegalChar;
    }

    public IllegalCharsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, char illegalChar) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.illegalChar = illegalChar;
    }
}
