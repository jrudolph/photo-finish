package net.virtualvoid.facerecognition;

import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

public class Face extends Structure {
    public static class ByReference extends Face implements Structure.ByReference {}

    public long left,top,right,bottom;
    public float[] model = new float[128];

    protected List<String> getFieldOrder() {
        return Arrays.asList("left", "top", "right", "bottom", "model");
    }
}