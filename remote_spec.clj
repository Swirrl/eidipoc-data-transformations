(ns nz-graft.remote-spec
  (:require [nz-graft.remote :as remote]
            [nz-graft.uri :as nz-uri]
            [nz-graft.transform :as trans]
            [clojure.test :refer :all]
            ;;[ring.mock.request :as req]
            [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [nz-graft.fixtures :refer :all]))

(deftest monitoring-sites-spec
  (let [monitoring-sites-seq (remote/get-monitoring-sites-seq-from (monitoring-sites-fixture))
        monitoring-sites-seq-as-hashes (remote/get-monitoring-sites-seq-maps-from (monitoring-sites-fixture) :horizons)]

    (is (not (= first-site-hash
                (first monitoring-sites-seq-as-hashes)))) ;; first-site-hash would be the first in the feed but is validated out

    (is (= 378
           (count monitoring-sites-seq))) ;; tests validation

    (is (not (= (count monitoring-sites-seq)
                (count monitoring-sites-seq-as-hashes))))

    (is (= first-valid-site-hash
           (first monitoring-sites-seq-as-hashes)))
    
    (is (= expected-number-of-sites
           (count monitoring-sites-seq-as-hashes)))))

(deftest feed-point-spec
  (let [points-from-feed-fixture-via-dive (remote/get-points-seq-from (example-feed-fixture))
        first-point-from-feed-fixture (second points-from-feed-fixture-via-dive) ;; first item is metadata
        feed-fixture-as-hashes (remote/get-points-seq-maps-from (example-feed-fixture) :horizons)]
    (is (= (xml/parse-str first-point-fixture)
           first-point-from-feed-fixture))

    (is (= expected-number-of-results-including-metadata
           (count points-from-feed-fixture-via-dive)))

    (is (= first-point-hash
           (first feed-fixture-as-hashes)))))

(deftest feed-uri-spec
  (let [generated-uri (nz-uri/get-site-feed-uri :horizons
                                                {:feature-of-interest "Manawatu at Teachers College"
                                                 :property :stage
                                                 :from (trans/->nz-dt-str 2017 3 1)
                                                 :to (trans/->nz-dt-str 2017 3 14)})]

    (is (= uri-fixture
           generated-uri))))
