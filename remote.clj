(ns nz-graft.remote
  (:require [clj-http.client :as client]
            [clojure.data.xml :as xml]
            [nz-graft.uri :as nz-uri]
            [nz-graft.transform :refer :all]
            [nz-graft.map-transforms :refer :all]
            [ring.util.codec :as codec]
            [nz-graft.xml-utils :refer [filter-by-tag
                                        first-with-tag
                                        first-content-by-tag
                                        descend-xml-tree]]))

(defn get-body-as-xml [location]
  "This can be used as a parsed xml repository"
  (let [xml-repo (-> (client/get location)
                     :body
                     (xml/parse-str))]
    xml-repo))

(defn get-points-seq-from [xml-repo]
  "Returns a seq of xml points records. Bear in mind the first will be metadata
   and then the actual points/observations will follow.
   Takes a parsed xml payload or file"
      (descend-xml-tree [:observationMember :OM_Observation :result :MeasurementTimeseries]
                        xml-repo))

(defn get-points-seq-maps-from [xml-repo org]
  "Returns a sequence of maps representing the XML records.
   Omits any metadata values by filtering on :point
   Takes an xml repo and an org keyword
   Waikato is special cased because their xml is the wrong shape"
  (let [points-seq (if (= org :waikato)
                     (descend-xml-tree [:observationData :OM_Observation :result :MeasurementTimeseries]
                                       xml-repo)
                     (get-points-seq-from xml-repo))
        xfrm-fn (if (= org :waikato)
                  #(waikato-point-xml-record->map %)
                  #(point-xml-record->map %))]

    (->> points-seq
         (filter-by-tag :point)
         (map xfrm-fn)
         (map #(merge {:org org} %)))))

(defn remove-sites-without-flow-data [sites-maps]
  "The :sw-quantity key is our clue to whether or not there is flow data.
   It has already been downcased, so any value other than 'yes' is an entry
   that we do not want to include"
  (filter #(= "yes" (:sw-quantity %)) sites-maps))

(defn get-monitoring-sites-seq-from [xml-repo]
  (->> xml-repo
       :content
       (filter-by-tag :featureMember)))

(defn get-monitoring-sites-seq-maps-from [xml-repo org]
  (let [xfrm-fn (case org
                  :horizons #(horizons-monitoring-site-xml-record->map %)
                  :canterbury #(canterbury-monitoring-site-xml-record->map %)
                  :hawkes-bay #(hawkes-bay-monitoring-site-xml-record->map %)
                  :waikato #(waikato-monitoring-site-xml-record->map %))]
   (->> (get-monitoring-sites-seq-from xml-repo)
        (map xfrm-fn)
        remove-sites-without-flow-data))) ;; we never actually want to add a site to the DS unless it has flow data

