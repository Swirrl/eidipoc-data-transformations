(ns nz-graft.templates.reach
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

(def reaches-graft
  (graph-fn [{geometry "a"
              object-id "b"
              from-node "w"
              to-node "x"
              length "e"
              reach "j"
              order "i"
              distsea "y"
              catcharea "z"
              climate "k"
              src-of-flow "l"
              geology "m"
              landcover "n"
              net-posn "o"
              vly-landfrm "p"
              :as row}]
            (let [graph-uri (get row "graph-uri")
                  reach-uri (pre/reach-uri reach)
                  geometry-uri (pre/reach-geometry-uri reach)
                  from-node-uri (pre/node-uri from-node)
                  to-node-uri (pre/node-uri to-node)
                  climate-uri (pre/climate-uri climate)
                  sof-uri (pre/src-of-flow-uri src-of-flow)
                  geology-uri (pre/geology-uri geology)
                  landcover-uri (pre/landcover-uri landcover)
                  net-pos-uri (pre/net-posn-uri net-posn)
                  valley-landform-uri (pre/vly-landfrm-uri vly-landfrm)]

              (graph graph-uri
                     [reach-uri
                      [rdf:a pre/nzdef:Reach]
                      [rdfs:label (literal-if-present reach pre/xsd:string)]
                      [pre/nzdef:reachID (literal-if-present reach pre/xsd:string)]
                      [pre/geosparql:hasGeometry geometry-uri]
                      [pre/nzdef:fromNode from-node-uri]
                      [pre/nzdef:toNode to-node-uri]
                      [pre/nzdef:distsea (literal-if-present distsea
                                                             pre/xsd:double)]
                      [pre/nzdef:order (literal-if-present order
                                                           pre/xsd:string)]
                      [pre/nzdef:length (literal-if-present length
                                                            pre/xsd:double)]
                      [pre/nzdef:catchmentArea (literal-if-present catcharea
                                                                   pre/xsd:double)]
                      [pre/nzdef:climate climate-uri]
                      [pre/nzdef:sourceOfFlow sof-uri]
                      [pre/nzdef:geology geology-uri]
                      [pre/nzdef:landcover landcover-uri]
                      [pre/nzdef:netPosition net-pos-uri]
                      [pre/nzdef:valleyLandform valley-landform-uri]]

                     [geometry-uri
                      [rdf:a pre/geosparql:Geometry]
                      [pre/geosparql:asWKT (grafter.rdf/literal geometry
                                                                pre/geosparql:wktLiteral)]]

                     [from-node-uri
                      [rdf:a pre/nzdef:Node]
                      [rdfs:label from-node]
                      [pre/nzdef:nodeID from-node]]

                     [to-node-uri
                      [rdf:a pre/nzdef:Node]
                      [rdfs:label to-node]
                      [pre/nzdef:nodeID to-node]]))))