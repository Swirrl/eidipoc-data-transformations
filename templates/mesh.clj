(ns nz-graft.templates.mesh
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

(defn mesh-graft [maps-seq]
  (mapcat (fn [{label :label
                geometry :geometry
                uri :uri
                type :type}]
            (let [graph-uri (pre/base-graph "mesh-blocks")
                  geometry-uri (-> label
                                   pre/mesh-geometry-uri)]
              (if-not (nil? geometry)
                (graph graph-uri
                       [uri
                        [rdf:a type]
                        [rdfs:label (grafter.rdf/literal label
                                                         pre/xsd:string)]
                        [pre/geosparql:hasGeometry geometry-uri]]
                       
                       [geometry-uri
                        [rdf:a pre/geosparql:Geometry]
                        [pre/geosparql:asWKT (grafter.rdf/literal geometry
                                                                  pre/geosparql:wktLiteral)]]))))
          maps-seq))
