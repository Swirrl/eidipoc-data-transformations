(ns nz-graft.pipelines.reach
  (:require [grafter.rdf.protocols :refer [->Quad]]
            [grafter.pipeline :refer [declare-pipeline]]
            [grafter.tabular :as tabular]
            [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz]]
            [nz-graft.utils :refer [validate-quads write-pipeline-to-nt-file]]
            [nz-graft.templates.reach :refer [reaches-graft]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn reaches-pipeline [data-graph-uri csv-dataset]
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        dataset (-> (tabular/read-dataset csv-dataset)
                    ;; (tabular/drop-rows 1) ;; split csv so headers no longer relevant
                    (tabular/add-columns {"graph-uri" graph-uri}))]
    (-> (reaches-graft dataset)
        validate-quads)))

(defn write-reaches-pipeline-to-nt-file [csv-location dest]
  (grafter.rdf/add (grafter.rdf.io/rdf-serializer dest
                                                  :format
                                                  :nt)
                   (doall (reaches-pipeline (pre/base-data "reaches")
                                            csv-location))))
