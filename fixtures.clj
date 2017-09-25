(ns nz-graft.fixtures
  (:require [nz-graft.remote :as remote]
            [nz-graft.uri :as nz-uri]
            [nz-graft.transform :as trans]
            [clojure.test :refer :all]
            ;;[ring.mock.request :as req]
            [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [nz-graft.prefix :as pre]))

(defn monitoring-sites-fixture []
  (let [input (io/reader "data/horizon_monitoring_sites.xml")]
    (doall (xml/parse input))))

(defn monitoring-sites-maps-fixture []
  (let [input (io/reader "data/horizon_monitoring_sites.xml")
        xml-repo (doall (xml/parse input))]
    (remote/get-monitoring-sites-seq-maps-from xml-repo :horizons)))

(defn example-feed-fixture []
  (let [input (io/reader "data/example_monitoring_feed.xml")]
    (doall (xml/parse input))))

(defn example-feed-maps-fixture []
  (let [input (io/reader "data/example_monitoring_feed.xml")
        xml-repo (doall (xml/parse input))]
    (remote/get-points-seq-maps-from xml-repo :horizons)))

(def first-point-fixture "<?xml version='1.0' encoding='UTF-8'?><point><MeasurementTVP><time>2017-03-01T00:00:00+12:00</time><value>543</value></MeasurementTVP></point>")

(def uri-fixture "http://tsdata.horizons.govt.nz/boo.hts?service=SOS&request=GetObservation&featureOfInterest=Manawatu%20at%20Teachers%20College&observedProperty=Stage%20%5BWater%20Level%5D&temporalFilter=om%3AphenomenonTime%2C2017-03-01T00%3A00%3A00.000%2B12%3A00%2F2017-03-14T00%3A00%3A00.000%2B12%3A00")

(def expected-number-of-results-including-metadata 3745)

(def first-point-hash {:org :horizons
                       :value "543"
                       :time "2017-03-01T00:00:00+12:00"})

(def expected-number-of-sites 41)

(def expected-number-of-monitoring-site-quads 915)

(def expected-number-of-feed-quads 33696)

(def horizons-graph-uri (pre/base-graph "horizons-monitoring-sites"))

(def horizons-first-site-uri (java.net.URI. "http://envdatapoc.co.nz/id/measurement-site/HRC-00001"))

(def horizons-first-valid-site-uri (java.net.URI. "http://envdatapoc.co.nz/id/measurement-site/HRC-00003"))

(def first-site-hash
  {:long "175.24728000000016"
   :council-site-id "Arawhata Drain at Hokio Beach Road"
   :elevation nil
   :catchment nil
   :alt-site-id "Arawhata Drain at Hokio Beach Road"
   :reach "7046139"
   :lawa-site-id "HRC-00001"
   :photograph nil
   :lat "-40.621899999999926"
   :site-id "Arawhata Drain at Hokio Beach Road"
   :management-zone nil}) ;; this should be validated out in remote

(def first-valid-site-hash
  {:long "175.78086000000008"
   :council-site-id "Hautapu at Alabasters"
   :elevation "445"
   :catchment "Rangitikei"
   :malf1d nil
   :alt-site-id "Hautapu at Alabasters"
   :malf7d nil
   :reach "7023817"
   :sw-quantity "yes"
   :mean-annual-flow "4.46"
   :mean-annual-flood-flow "46.192999999999998"
   :max-recorded-flow "206.40899999999999"
   :min-recorded-flow "0.27200000000000002"
   :lawa-site-id "HRC-00003"
   :photograph "http://hilltopserver.horizons.govt.nz/images/sitephotos/Hautapu at Alabasters.jpg"
   :malf "0.745"
   :lat "-39.656880000000001"
   :site-id "Hautapu at Alabasters"
   :management-zone "Rang"
   :median-annual-flow "2.3999999999999999"})

(def first-feed-hash {:time "foo" :value "100"})

(def first-observation-uri (java.net.URI. "http://envdatapoc.co.nz/data/HRC-00001/stage/2017-03-01T00:00:00+12:00"))
