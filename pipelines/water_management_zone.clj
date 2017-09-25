(ns nz-graft.pipelines.water-management-zone
  (:require [grafter.rdf.protocols :refer [->Quad]]
            [grafter.pipeline :refer [declare-pipeline]]
            [grafter.tabular :as tabular]
            [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz]]
            [nz-graft.utils :refer [validate-quads write-pipeline-to-nt-file]]
            [nz-graft.templates.wmz :refer [water-management-zone-graft]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(def horizons-location "data/wmzs/horizons.csv")
(def hawkes-bay-location "data/wmzs/hawkesbay.csv")
(def waikato-location "data/wmzs/waikato.csv")

(defn get-dataset-for [region]
  (let [dataset-location (case region
                           :horizons horizons-location
                           :hawkes-bay hawkes-bay-location
                           :waikato waikato-location)]
    (-> (tabular/read-dataset dataset-location)
        (tabular/drop-rows 1) ;; csv so row 1 is actually header
        )))

(defn horizons-wmz-pipeline []
  (let [graph-uri (pre/base-graph "horizons-water-management-zones")
        dataset (-> (get-dataset-for :horizons)
                    (tabular/add-columns {"graph-uri" graph-uri 
                                          "region" "horizons"}))]
    (water-management-zone-graft dataset)))

(defn hawkes-bay-wmz-pipeline []
  (let [graph-uri (pre/base-graph "hawkes-bay-water-management-zones")
        dataset (-> (get-dataset-for :hawkes-bay)
                    (tabular/add-columns {"graph-uri" graph-uri 
                                          "region" "hawkes-bay"}))]
    (water-management-zone-graft dataset)))

(defn waikato-wmz-pipeline []
  (let [graph-uri (pre/base-graph "waikato-water-management-zones")
        dataset (-> (get-dataset-for :waikato)
                    (tabular/add-columns {"graph-uri" graph-uri 
                                          "region" "waikato"}))]
    (water-management-zone-graft dataset)))

