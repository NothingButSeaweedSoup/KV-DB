package util;

public final class ByteUtil {
    private ByteUtil() {}

    public static int compare(byte[] a, byte[] b) {
        if(a == null || b == null){
            throw new NullPointerException();
        }
        int len = Math.min(a.length, b.length);
        for(int i = 0; i < len; i++){
            int delta = (a[i] & 0xFF)-(b[i] & 0xFF);
            if(delta!= 0){
                return delta;
            }
        }
        return a.length - b.length;
    }
}
