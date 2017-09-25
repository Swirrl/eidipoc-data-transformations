(ns nz-graft.pipeline-spec
  (:require [nz-graft.pipeline :as pipelines]
            [nz-graft.uri :as nz-uri]
            [nz-graft.transform :as trans]
            [clojure.test :refer :all]
            ;;[ring.mock.request :as req]
            [nz-graft.prefix :as pre]
            [nz-graft.pipelines.sites :as sites-pipeline]
            [nz-graft.utils :as utils]
            [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [nz-graft.fixtures :refer :all]))

(deftest sites-helper-spec
  (let [{graph-uri :graph-uri
         site-maps :monitoring-site-maps} (sites-pipeline/get-monitoring-sites-maps-for :horizons)]
    (is (= expected-number-of-sites
           (count site-maps)))
    (is (= graph-uri
           horizons-graph-uri))))

(deftest horizon-sites-pipeline-spec
  (let [quads (sites-pipeline/run-sites-pipeline-with (pre/base-graph "horizons-monitoring-sites")
                                             (monitoring-sites-maps-fixture)) ;;(pre/base-data "horizons-monitoring-sites")
               
        first-quad (first quads)]
    (is (= expected-number-of-monitoring-site-quads
           (count quads)))

    (is (= (:s first-quad)
           horizons-first-valid-site-uri))

    (is (= 0
           (->> quads
                (filter #(= horizons-first-site-uri (:s %)))
                count)))))

(deftest horizon-stage-pipeline-spec
  (let [quads (->> (flatten (nz-graft.templates.feed/feed-graft "stage"
                                                                "HRC-00001"
                                                                (pre/base-graph "horizons-stage-measurements")
                                                                (example-feed-maps-fixture)))
                   utils/validate-quads)
        first-quad (first quads)]
    (is (= expected-number-of-feed-quads
           (count quads)))
    (is (= (:s first-quad)
           first-observation-uri))))

