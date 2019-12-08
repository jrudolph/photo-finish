package net.virtualvoid.facerecognition;

import com.sun.jna.Native;

/**
 * struct face {
 *   long left;
 *   long top;
 *   long right;
 *   long bottom;
 *   float model[128];
 * };
 * int detect_faces(const char *imageFile, struct face* retFaces, int maxFaces);
 */



public class FaceRecognitionLib {
    static {
        Native.register("face_recognition");
    }
    public static native int detect_faces(String imageFile, Face.ByReference retFaces, int maxFaces, int numJitters);
}
