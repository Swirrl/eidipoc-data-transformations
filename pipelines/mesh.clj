(ns nz-graft.pipelines.mesh
  (:require [grafter.rdf.protocols :refer [->Quad]]
            [grafter.pipeline :refer [declare-pipeline]]
            [grafter.tabular :as tabular]
            [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz]]
            [nz-graft.utils :refer [validate-quads write-pipeline-to-nt-file]]
            [nz-graft.templates.mesh :refer [mesh-graft]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (org.geotools.geojson.geom GeometryJSON)
           (org.geotools.geojson.feature FeatureJSON)
           (org.geotools.geometry.jts WKTWriter2)))

(defn get-properties [simplefeature]
  
  (let [dimensions 2
        wkt-writer (WKTWriter2. dimensions)
        label (.getValue (.getProperty simplefeature "MB2017"))
        geometry (.getDefaultGeometry simplefeature)
        wkt-geometry (try
                       (.write wkt-writer geometry)
                       (catch Exception e
                         (println label)))
        uri (-> label
                pre/mesh-uri)
        rdf-type pre/nzdef:MeshBlock]
    {:label label
     :geometry wkt-geometry
     :uri uri
     :type rdf-type}))

(defn read-mesh-geojson [mesh-file]
  (with-open [reader (io/reader mesh-file)]
    (.readFeatureCollection (FeatureJSON.)
                            reader)))

(defn get-properties-maps [mesh-file]
  (map #(get-properties %) (read-mesh-geojson mesh-file)))


(defn mesh-pipeline [mesh-file-path]
  (let [property-maps (get-properties-maps mesh-file-path)]
    (mesh-graft property-maps)))

(declare-pipeline mesh-pipeline
  "Import mesh file"
  [String -> (Seq Quad)]
  {mesh-file-path "The path to the mesh blocks file to convert.  These can be relative to the service directory e.g. ./data/mesh_blocks/MB2017_GV_Full.json"})



