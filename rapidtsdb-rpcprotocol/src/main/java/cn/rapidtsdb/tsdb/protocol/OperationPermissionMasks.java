package cn.rapidtsdb.tsdb.protocol;

public class OperationPermissionMasks {
    public static final int READ_PERMISSION = 1;
    public static final int WRITE_PERMISSION = 1 << 1;
    public static final int ADMIN_PERMISSION = 1 << 2;
    public static final int DEBUG_PERMISSION = 1 << 3; // danger, do not open in prod env
    public static final int RW_PERMISSION = READ_PERMISSION | WRITE_PERMISSION;
    public static final int RWM_PERMISSION = READ_PERMISSION | WRITE_PERMISSION | ADMIN_PERMISSION;


    public static boolean hadReadPermission(int mask) {
        return (mask & READ_PERMISSION) > 0;
    }

    public static boolean hadWritePermission(int mask) {
        return (mask & WRITE_PERMISSION) > 0;
    }

    public static boolean hadAdminPermission(int mask) {
        return (mask & ADMIN_PERMISSION) > 0;
    }

    public static boolean hadDebugPermission(int mask) {
        return false;
    }
}
