package cn.tinytsdb.tsdb.utils;

public class ClassUtils {

    public static boolean isAssignable(Class leftClass, Class rightClass) {
        if(leftClass==null || rightClass==null) {
            return false;
        }
        return leftClass.isAssignableFrom(rightClass);
    }


    public static Class getUserClass(Class clazz) {
        Class thisClazz = clazz;
        if(thisClazz.getSimpleName().contains("$$")) {
            System.err.println("supering... " + thisClazz);
            Class superclass = thisClazz.getSuperclass();
            if(superclass!=null && superclass!=Object.class) {
                return superclass;
            }
        }
        return thisClazz;
    }


}
