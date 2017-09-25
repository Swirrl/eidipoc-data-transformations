(ns nz-graft.pipelines.feeds
  (:require [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->nz-dt-str
                                        now-in-nz
                                        yesterday-in-nz
                                        one-hour-ago-in-nz
                                        now-utc
                                        one-hour-ago-utc
                                        two-hours-ago-utc
                                        two-days-ago-utc]]
            [nz-graft.remote :as remote]
            [nz-graft.uri :as nz-uri]
            [nz-graft.templates.feed :as template]
            [nz-graft.utils :refer :all]
            [nz-graft.pipelines.sites :refer [get-monitoring-sites-maps-for]]))

(defn get-point-maps-for [org uri]
  (-> uri
      remote/get-body-as-xml
      (remote/get-points-seq-maps-from org)))

(defn site->feed->quads [graph-uri monitoring-site-maps type org]
  "Type should be a string and org a keyword."
  (->> (mapcat (fn [{site-id :site-id
                     lawa-site-id :lawa-site-id
                     alt-site-id :alt-site-id}]
                 (let [feature-of-interest (case org
                                             :horizons site-id ;; the text site name
                                             :canterbury site-id
                                             :hawkes-bay site-id
                                             :waikato alt-site-id)
                       from-time (case org
                                   :horizons (two-hours-ago-utc)
                                   :canterbury (two-days-ago-utc)
                                   :hawkes-bay (two-hours-ago-utc)
                                   :waikato (two-hours-ago-utc))
                       point-location (nz-uri/get-site-feed-uri org
                                                                {:feature-of-interest feature-of-interest
                                                                 :property (keyword type)
                                                                 :from from-time
                                                                 :to (now-utc)})
                       point-maps (get-point-maps-for org point-location)]
                   (template/feed-graft type
                                        lawa-site-id
                                        graph-uri
                                        point-maps)))
               monitoring-site-maps)
       (into [] (mapcat identity))))

(defn run-feed-pipeline-for [type org data-graph-uri]
  "Where org is :horizons, :canterbury, :hawkes-bay or :waikato
   and type is 'stage' or 'flow'."
  (let [graph-uri (pre/data-graph-uri->graph-uri data-graph-uri)
        monitoring-site-maps (-> (get-monitoring-sites-maps-for org)
                                 :monitoring-site-maps)]
    (->> (site->feed->quads graph-uri
                            monitoring-site-maps
                            type
                            org)
         validate-quads)))
