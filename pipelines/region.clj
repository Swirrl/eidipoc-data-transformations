(ns nz-graft.pipelines.region
  (:require [grafter.rdf.protocols :refer [->Quad]]
            [grafter.pipeline :refer [declare-pipeline]]
            [grafter.tabular :as tabular]
            [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz]]
            [nz-graft.utils :refer [validate-quads write-pipeline-to-nt-file]]
            [nz-graft.templates.region :refer [regions-graft]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (org.geotools.geojson.geom GeometryJSON)
           (org.geotools.geojson.feature FeatureJSON)
           (org.geotools.geometry.jts WKTWriter2)))

(defn get-properties [simplefeature]
  (let [dimensions 2
        wkt-writer (WKTWriter2. dimensions)
        label (.getValue (.getProperty simplefeature "REGC2017_N"))
        geometry (.getDefaultGeometry simplefeature)
        wkt-geometry (.write wkt-writer geometry)
        uri (-> label
                (clojure.string/replace #"Region" "")
                pre/regional-council-uri)
        rdf-type pre/nzdef:Region]
    {:label label
     :geometry wkt-geometry
     :uri uri
     :type rdf-type}))

(defn read-regions-geojson []
  (with-open [reader (io/reader "/Users/the_frey/projects/swirrl/nz-graft/data/regions/REGC2017_GV_Full.json")]
    (.readFeatureCollection (FeatureJSON.)
                            reader)))

(defn get-properties-maps []
  (map #(get-properties %) (read-regions-geojson)))

(defn regions-pipeline []
  (let [property-maps (get-properties-maps)]
    (regions-graft property-maps)))

