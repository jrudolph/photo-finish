package net.virtualvoid.fotofinish.web

import net.virtualvoid.fotofinish.Hash

case class FaceNeighbor(targetHash: Hash, faceIdx: Int, distance: Float, title: String)
