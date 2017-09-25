(ns nz-graft.templates.feed
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

(defn feed-graft [type measurement-site-id graph-uri point-maps]
  (map (fn [{value :value
             time :time
             org :org}]
         
         (let [measurement-site-uri (pre/measurement-site-uri measurement-site-id)
               observation-uri (pre/observation-uri measurement-site-id type time)
               result-uri (pre/result-uri  measurement-site-id type time)
               result-value (if (and (= type "flow")
                                     (or (= org :horizons)
                                         (= org :hawkes-bay)))
                              (litre-per-second->cumec value)
                              value)
               observed-property (case type
                                   "stage" pre/stage-water-level
                                   "flow" pre/flow-water-level)
               result-unit (case type
                             "stage" pre/qudt-unit-1-1:Millimeter
                             "flow" pre/qudt-unit-1-1:CubicMeterPerSecond)]
           
           (graph graph-uri
                  [observation-uri
                   [rdf:a pre/sosa:Observation]
                   [rdfs:label (grafter.rdf/literal (str measurement-site-id
                                                         ": "
                                                         type
                                                         " at "
                                                         time)
                                                    pre/xsd:string)]
                   [pre/sosa:observedProperty observed-property]
                   [pre/sosa:hasFeatureOfInterest measurement-site-uri]
                   [pre/sosa:hasResult result-uri]
                   [pre/sosa:resultTime (grafter.rdf/literal time
                                                             pre/xsd:dateTime)]]
                  
                  [result-uri
                   [rdf:a pre/qudt-1-1:QuantityValue]
                   [pre/qudt-1-1:unit result-unit]
                   [pre/qudt-1-1:numericValue (grafter.rdf/literal result-value
                                                                   pre/xsd:double)]])))
       point-maps))