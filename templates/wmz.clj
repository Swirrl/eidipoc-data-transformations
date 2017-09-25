(ns nz-graft.templates.wmz
    (:require
     [grafter.tabular :refer [_ add-column add-columns apply-columns
                              build-lookup-table column-names columns
                              derive-column drop-rows graph-fn grep make-dataset
                              mapc melt move-first-row-to-header read-dataset
                              read-datasets rows swap swap take-rows
                              test-dataset test-dataset]]
     [grafter.rdf.protocols :refer [->Quad]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.pipeline :refer [declare-pipeline]]
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [nz-graft.prefix :as pre]
     [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz litre-per-second->cumec]]
     [nz-graft.remote :as remote]
     [nz-graft.uri :as nz-uri]
     [publish-my-data.pipelines.parameter-types :as pmd-types]
     [nz-graft.templates.template-utils :refer :all]))

(def water-management-zone-graft
  (graph-fn [row]
            (let [graph-uri (get row "graph-uri")
                  region (get row "region")
                  water-management-zone-id (case region
                                             "horizons" (get row "d")
                                             "hawkes-bay" (get row "f")
                                             "waikato" (get row "c"))
                  geometry (get row "a") ;; WKT is always in 'a' col
                  water-management-zone-uri (pre/water-management-zone-uri water-management-zone-id)
                  geometry-uri (pre/wmz-geometry-uri water-management-zone-id)]

              (graph graph-uri
                     [water-management-zone-uri
                      [rdf:a pre/nzdef:WaterManagementZone]
                      [rdfs:label water-management-zone-id]
                      [pre/geosparql:hasGeometry geometry-uri]]

                     [geometry-uri
                      [rdf:a pre/geosparql:Geometry]
                      [pre/geosparql:asWKT (grafter.rdf/literal geometry
                                                                pre/geosparql:wktLiteral)]]))))