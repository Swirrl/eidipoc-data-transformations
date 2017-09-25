(ns nz-graft.pipelines.sites
  (:require [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->nz-dt-str
                                        now-in-nz
                                        yesterday-in-nz
                                        one-hour-ago-in-nz
                                        now-utc
                                        one-hour-ago-utc
                                        two-hours-ago-utc]]
            [nz-graft.remote :as remote]
            [nz-graft.uri :as nz-uri]
            [nz-graft.templates.site :as template]
            [nz-graft.utils :refer :all]))

(defn get-monitoring-sites-maps-for [site]
  (let [graph-uri (case site
                    :horizons (pre/base-graph "horizons-monitoring-sites") 
                    :canterbury (pre/base-graph "canterbury-monitoring-sites")
                    :hawkes-bay (pre/base-graph "hawkes-bay-monitoring-sites")
                    :waikato (pre/base-graph "waikato-monitoring-sites"))
        xml-repo (case site
                    :horizons (remote/get-body-as-xml nz-uri/horizons-monitoring-sites)
                    :canterbury (remote/get-body-as-xml nz-uri/canterbury-monitoring-sites)
                    :hawkes-bay (remote/get-body-as-xml nz-uri/hawkes-bay-monitoring-sites)
                    :waikato (remote/get-body-as-xml nz-uri/waikato-monitoring-sites))
        monitoring-site-maps (remote/get-monitoring-sites-seq-maps-from xml-repo site)]
    {:graph-uri graph-uri
     :monitoring-site-maps monitoring-site-maps}))

(defn run-sites-pipeline-with [graph-uri monitoring-site-maps]
  (->> monitoring-site-maps
       (template/sites-graft graph-uri)
       validate-quads))
