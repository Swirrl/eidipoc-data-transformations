(ns nz-graft.pipeline
  (:require [grafter.rdf.protocols :refer [->Quad]]
            [grafter.pipeline :refer [declare-pipeline]]
            [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz]]
            [publish-my-data.pipelines.parameter-types :as pmd-types]
            [nz-graft.utils :refer [validate-quads]]
            [nz-graft.pipelines.sites :refer :all]
            [nz-graft.pipelines.feeds :refer :all]
            [nz-graft.pipelines.reach :refer :all]))

;; one per authority 
(defn horizons-sites-pipeline [data-graph-uri]
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        {monitoring-site-maps :monitoring-site-maps} (get-monitoring-sites-maps-for :horizons)]
    (run-sites-pipeline-with graph-uri monitoring-site-maps)))

(defn canterbury-sites-pipeline [data-graph-uri]
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        {monitoring-site-maps :monitoring-site-maps} (get-monitoring-sites-maps-for :canterbury)]
    (run-sites-pipeline-with graph-uri monitoring-site-maps)))

(defn hawkes-bay-sites-pipeline [data-graph-uri]
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        {monitoring-site-maps :monitoring-site-maps} (get-monitoring-sites-maps-for :hawkes-bay)]
    (run-sites-pipeline-with graph-uri monitoring-site-maps)))

(defn waikato-sites-pipeline [data-graph-uri]
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        {monitoring-site-maps :monitoring-site-maps} (get-monitoring-sites-maps-for :waikato)]
    (run-sites-pipeline-with graph-uri monitoring-site-maps)))

;; pipelines
(declare-pipeline horizons-sites-pipeline
  "Import sites data for Horizons"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline canterbury-sites-pipeline 
  "Import sites data for Canterbury"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline hawkes-bay-sites-pipeline
  "Import sites data for Hawkes Bay"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline waikato-sites-pipeline
  "Import sites data for Waikato"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

;; takes a parameter of "stage" or "flow" for type
(defn horizons-stage-pipeline [data-graph-uri]
  (run-feed-pipeline-for "stage" :horizons data-graph-uri))

(defn horizons-flow-pipeline [data-graph-uri]
  (run-feed-pipeline-for "flow" :horizons data-graph-uri))

(defn canterbury-stage-pipeline [data-graph-uri]
  (run-feed-pipeline-for "stage" :canterbury data-graph-uri))

(defn canterbury-flow-pipeline [data-graph-uri]
  (run-feed-pipeline-for "flow" :canterbury data-graph-uri))

(defn hawkes-bay-stage-pipeline [data-graph-uri]
  (run-feed-pipeline-for "stage" :hawkes-bay data-graph-uri))

(defn hawkes-bay-flow-pipeline [data-graph-uri]
  (run-feed-pipeline-for "flow" :hawkes-bay data-graph-uri))

(defn waikato-stage-pipeline [data-graph-uri]
  (run-feed-pipeline-for "stage" :waikato data-graph-uri))

(defn waikato-flow-pipeline [data-graph-uri]
  (run-feed-pipeline-for "flow" :waikato data-graph-uri))

;; declare pipelines
(declare-pipeline horizons-stage-pipeline
  "Import stage data for Horizons"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline horizons-flow-pipeline
  "Import flow data for Horizons"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline canterbury-stage-pipeline
  "Import stage data for Canterbury"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline canterbury-flow-pipeline
  "Import flow data for Canterbury"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline hawkes-bay-stage-pipeline
  "Import stage data for Hawkes Bay"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline hawkes-bay-flow-pipeline
  "Import flow data for Hawkes Bay"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline waikato-stage-pipeline
  "Import stage data for Waikato"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

(declare-pipeline waikato-flow-pipeline
  "Import flow data for Waikato"
  [pmd-types/dataset-uri -> (Seq Quad)]
  {data-graph-uri "Target DS"})

;; reach graft

(declare-pipeline reaches-pipeline
  "From a reach CSV file download, create triples"
  [pmd-types/dataset-uri incanter.core.Dataset -> (Seq Quad)]
  {data-graph-uri "Target DS"
   csv-dataset "A dataset in CSV format"})
