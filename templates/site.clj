(ns nz-graft.templates.site
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

(defn sites-graft [graph-uri site-maps]
  (->> (mapcat (fn [{site-id :site-id
                     long :long
                     lat :lat
                     elevation :elevation
                     council-site-id :council-site-id
                     lawa-site-id :lawa-site-id
                     management-zone :management-zone
                     catchment :catchment
                     photograph :photograph
                     reach :reach
                     mean-annual-flow :mean-annual-flow
                     median-annual-flow :median-annual-flow
                     min-recorded-flow :min-recorded-flow
                     max-recorded-flow :max-recorded-flow
                     mean-annual-flood-flow :mean-annual-flood-flow
                     malf :malf
                     malf1d :malf1d
                     malf7d :malf7d}]

                 (if lawa-site-id ;; validate to eliminate wells that don't have site IDs
                   (let [wkt-literal-vector (if (and lat long)
                                              [pre/geosparql:asWKT (grafter.rdf/literal (str "POINT (" long " " lat ")")
                                                                                        pre/geosparql:wktLiteral)])
                         geometry-uri (when wkt-literal-vector
                                        (pre/measurement-site-geometry lawa-site-id))
                         
                         catchment-uri (when catchment
                                         (pre/catchment-uri catchment))
                         water-management-zone-uri (when management-zone
                                                     (pre/water-management-zone-uri management-zone))
                         reach-uri (when reach
                                     (pre/reach-uri reach))

                         mean-annual-flow-uri (when mean-annual-flow
                                                (pre/long-term-flow-observation-uri "mean-annual-flow" lawa-site-id))
                         median-annual-flow-uri (when median-annual-flow
                                                  (pre/long-term-flow-observation-uri "median-annual-flow" lawa-site-id))
                         min-recorded-flow-uri (when min-recorded-flow
                                                 (pre/long-term-flow-observation-uri "min-recorded-flow" lawa-site-id))
                         max-recorded-flow-uri (when max-recorded-flow
                                                 (pre/long-term-flow-observation-uri "max-recorded-flow" lawa-site-id))
                         mean-annual-flood-flow-uri (when mean-annual-flood-flow
                                                      (pre/long-term-flow-observation-uri "mean-annual-flood-flow" lawa-site-id))
                         malf-uri (when malf
                                    (pre/long-term-flow-observation-uri "malf" lawa-site-id))
                         malf1d-uri (when malf1d
                                      (pre/long-term-flow-observation-uri "malf1d" lawa-site-id))
                         malf7d-uri (when malf7d
                                      (pre/long-term-flow-observation-uri "malf7d" lawa-site-id))

                         geometry-resource (when wkt-literal-vector
                                             [geometry-uri
                                              [rdf:a pre/geosparql:Geometry]
                                              wkt-literal-vector])
                         catchment-resource (when catchment-uri
                                              [catchment-uri
                                               [rdfs:label (literal-if-present catchment
                                                                               pre/xsd:string)]])
                         reach-resource (when reach-uri
                                          [reach-uri
                                           [rdfs:label (literal-if-present reach
                                                                           pre/xsd:string)]])

                         mean-annual-flow-resource (long-term-flow-resource mean-annual-flow-uri
                                                                            mean-annual-flow)
                         median-annual-flow-resource (long-term-flow-resource median-annual-flow-uri
                                                                              median-annual-flow)
                         min-recorded-flow-resource (long-term-flow-resource min-recorded-flow-uri
                                                                             min-recorded-flow)
                         max-recorded-flow-resource (long-term-flow-resource max-recorded-flow-uri
                                                                             max-recorded-flow)
                         mean-annual-flood-flow-resource (long-term-flow-resource mean-annual-flood-flow-uri
                                                                                  mean-annual-flood-flow)
                         malf-resource (long-term-flow-resource malf-uri
                                                                malf)
                         malf1d-resource (long-term-flow-resource malf1d-uri
                                                                  malf1d)
                         malf7d-resource (long-term-flow-resource malf7d-uri
                                                                  malf7d)]
                     
                    (graph graph-uri
                           [(pre/measurement-site-uri lawa-site-id)
                            [rdf:a pre/sosa:FeatureOfInterest]
                            [rdf:a pre/nzdef:MeasurementSite]
                            [rdfs:label (grafter.rdf/literal site-id
                                                             pre/xsd:string)]
                            [pre/nzdef:siteID (grafter.rdf/literal site-id
                                                                   pre/xsd:string)]
                            [pre/geo:long long]
                            [pre/geo:lat lat]
                            [pre/nzdef:elevation (literal-if-present elevation
                                                                     pre/xsd:double)]
                            [pre/nzdef:councilSiteID (literal-if-present council-site-id
                                                                         pre/xsd:string)]
                            [pre/nzdef:lawaSiteID (literal-if-present lawa-site-id
                                                                      pre/xsd:string)]
                            [pre/nzdef:managementZone water-management-zone-uri]
                            [pre/nzdef:catchment catchment-uri]
                            [pre/nzdef:reach reach-uri]
                            [pre/schema:image photograph]
                            [pre/geosparql:hasGeometry geometry-uri]

                            [pre/nzdef:meanAnnualFlow mean-annual-flow-uri]
                            [pre/nzdef:medianAnnualFlow median-annual-flow-uri]
                            [pre/nzdef:minRecordedFlow min-recorded-flow-uri]
                            [pre/nzdef:maxRecordedFlow max-recorded-flow-uri]
                            [pre/nzdef:meanAnnualFloodFlow mean-annual-flood-flow-uri]
                            [pre/nzdef:MALF malf-uri]
                            [pre/nzdef:MALF1d malf1d-uri]
                            [pre/nzdef:MALF7d malf7d-uri]]
                           
                           catchment-resource
                           reach-resource
                           geometry-resource

                           mean-annual-flow-resource
                           median-annual-flow-resource
                           min-recorded-flow-resource
                           max-recorded-flow-resource
                           mean-annual-flood-flow-resource
                           malf-resource
                           malf1d-resource
                           malf7d-resource
                           ))))
               site-maps)
       (into [])))