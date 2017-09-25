(ns nz-graft.map-transforms
  (:require [nz-graft.xml-utils :refer [filter-by-tag
                                        first-with-tag
                                        first-content-by-tag
                                        descend-xml-tree]]
            [nz-graft.transform :as xfrm]))

(defn point-xml-record->map [record]
  (let [value (first (descend-xml-tree [:MeasurementTVP :value] record))
        time (xfrm/sanitize-time-for (first (descend-xml-tree [:MeasurementTVP :time] record))
                                     :not-waikato)]
    {:value value
     :time time}))

(defn waikato-point-xml-record->map [record]
  (let [value (first (descend-xml-tree [:MeasurementTVP :value] record))
        time (xfrm/sanitize-time-for (first (descend-xml-tree [:MeasurementTVP :time] record))
                                     :waikato)]
    {:value value
     :time time}))

(defn get-point-vec-from [point]
  (-> point
      (clojure.string/split #"\s")))

(defn horizons-monitoring-site-xml-record->map [record]
  (let [site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :SiteID] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        point (get-point-vec-from (first (descend-xml-tree [:MonitoringSiteReferenceData :Shape :Point :pos] record))) 
        elevation (first (descend-xml-tree [:MonitoringSiteReferenceData :Elevation] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        lawa-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :LawaSiteID] record))
        management-zone (first (descend-xml-tree [:MonitoringSiteReferenceData :SWManagementZone] record))
        catchment (first (descend-xml-tree [:MonitoringSiteReferenceData :Catchment] record))
        photograph (first (descend-xml-tree [:MonitoringSiteReferenceData :Photograph] record))
        reach (first (descend-xml-tree [:MonitoringSiteReferenceData :NZReach] record))
        mean-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFlow] record))
        median-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MedianAnnualFlow] record))
        min-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MinRecordedFlow] record))
        max-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MaxRecordedFlow] record))
        mean-annual-flood-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFloodFlow] record))
        malf (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF] record))
        malf1d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF1d] record))
        malf7d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF7d] record))
        sw-quantity (-> (first (descend-xml-tree [:MonitoringSiteReferenceData :SWQuantity] record))
                        clojure.string/lower-case
                        clojure.string/trim) ;; sanitize so we can filter later 
        lat (first point)
        long (second point)]

    {:alt-site-id site-id
     :site-id council-site-id
     :lat lat
     :long long
     :elevation elevation
     :council-site-id council-site-id
     :lawa-site-id lawa-site-id
     :management-zone management-zone
     :catchment catchment
     :photograph photograph
     :reach reach
     :mean-annual-flow mean-annual-flow
     :median-annual-flow median-annual-flow
     :min-recorded-flow min-recorded-flow
     :max-recorded-flow max-recorded-flow
     :mean-annual-flood-flow mean-annual-flood-flow
     :malf malf
     :malf1d malf1d
     :malf7d malf7d
     :sw-quantity sw-quantity}))

(defn canterbury-monitoring-site-xml-record->map [record]
  (let [site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :SiteID] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        point (get-point-vec-from (first (descend-xml-tree [:MonitoringSiteReferenceData :Shape :Point :pos] record))) 
        elevation (first (descend-xml-tree [:MonitoringSiteReferenceData :Elevation] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        lawa-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :LawaSiteID] record))
        management-zone (first (descend-xml-tree [:MonitoringSiteReferenceData :SWManagementZone] record))
        catchment (first (descend-xml-tree [:MonitoringSiteReferenceData :Catchment] record))
        photograph (first (descend-xml-tree [:MonitoringSiteReferenceData :Photograph] record))
        reach (first (descend-xml-tree [:MonitoringSiteReferenceData :NZReach] record))
                mean-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFlow] record))
        median-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MedianAnnualFlow] record))
        min-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MinRecordedFlow] record))
        max-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MaxRecordedFlow] record))
        mean-annual-flood-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFloodFlow] record))
        malf (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF] record))
        malf1d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF1d] record))
        malf7d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF7d] record))
        sw-quantity (-> (first (descend-xml-tree [:MonitoringSiteReferenceData :SWQuantity] record))
                        clojure.string/lower-case
                        clojure.string/trim)
        lat (second point) ;; backwards compared to all other regions
        long (first point)]

    {:alt-site-id site-id
     :site-id council-site-id
     :lat lat
     :long long
     :elevation elevation
     :council-site-id council-site-id
     :lawa-site-id lawa-site-id
     :management-zone management-zone
     :catchment catchment
     :photograph photograph
     :reach reach
     :mean-annual-flow mean-annual-flow
     :median-annual-flow median-annual-flow
     :min-recorded-flow min-recorded-flow
     :max-recorded-flow max-recorded-flow
     :mean-annual-flood-flow mean-annual-flood-flow
     :malf malf
     :malf1d malf1d
     :malf7d malf7d
     :sw-quantity sw-quantity}))

(defn hawkes-bay-monitoring-site-xml-record->map [record]
  (let [site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :SiteID] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        point (get-point-vec-from (first (descend-xml-tree [:MonitoringSiteReferenceData :SHAPE :Point :pos] record))) 
        elevation (first (descend-xml-tree [:MonitoringSiteReferenceData :Elevation] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :CouncilSiteID] record))
        lawa-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :LawaSiteID] record))
        management-zone (first (descend-xml-tree [:MonitoringSiteReferenceData :SWManagementZone] record))
        catchment (first (descend-xml-tree [:MonitoringSiteReferenceData :Catchment] record))
        photograph (first (descend-xml-tree [:MonitoringSiteReferenceData :Photograph] record))
        reach (first (descend-xml-tree [:MonitoringSiteReferenceData :NZReach] record))
        mean-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFlow] record))
        median-annual-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MedianAnnualFlow] record))
        min-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MinRecordedFlow] record))
        max-recorded-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MaxRecordedFlow] record))
        mean-annual-flood-flow (first (descend-xml-tree [:MonitoringSiteReferenceData :MeanAnnualFloodFlow] record))
        malf (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF] record))
        malf1d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF1d] record))
        malf7d (first (descend-xml-tree [:MonitoringSiteReferenceData :MALF7d] record))
        sw-quantity (-> (first (descend-xml-tree [:MonitoringSiteReferenceData :SWQuantity] record))
                        clojure.string/lower-case
                        clojure.string/trim)
        lat (first point)
        long (second point)]

    {:alt-site-id site-id
     :site-id council-site-id
     :lat lat
     :long long
     :elevation elevation
     :council-site-id council-site-id
     :lawa-site-id lawa-site-id
     :management-zone management-zone
     :catchment catchment
     :photograph photograph
     :reach reach
     :mean-annual-flow mean-annual-flow
     :median-annual-flow median-annual-flow
     :min-recorded-flow min-recorded-flow
     :max-recorded-flow max-recorded-flow
     :mean-annual-flood-flow mean-annual-flood-flow
     :malf malf
     :malf1d malf1d
     :malf7d malf7d
     :sw-quantity sw-quantity}))

(defn waikato-monitoring-site-xml-record->map [record]
  (let [site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :SITEID] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :COUNCILSITEID] record))
        point (get-point-vec-from (first (descend-xml-tree [:MonitoringSiteReferenceData :SHAPE :Point :pos] record))) 
        elevation (first (descend-xml-tree [:MonitoringSiteReferenceData :ELEVATION] record))
        council-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :COUNCILSITEID] record))
        lawa-site-id (first (descend-xml-tree [:MonitoringSiteReferenceData :LAWASITEID] record))
        management-zone (first (descend-xml-tree [:MonitoringSiteReferenceData :SWMANAGEMENTZONE] record))
        catchment (first (descend-xml-tree [:MonitoringSiteReferenceData :CATCHMENTAREA] record))
        photograph (first (descend-xml-tree [:MonitoringSiteReferenceData :PHOTOGRAPH] record))
        reach (first (descend-xml-tree [:MonitoringSiteReferenceData :NZREACH] record))
        sw-quantity (-> (first (descend-xml-tree [:MonitoringSiteReferenceData :SWQUANTITY] record))
                        clojure.string/lower-case
                        clojure.string/trim)
        lat (first point)
        long (second point)]

    {:alt-site-id council-site-id
     :site-id site-id
     :lat lat
     :long long
     :elevation elevation
     :council-site-id council-site-id
     :lawa-site-id lawa-site-id
     :management-zone management-zone
     :catchment catchment
     :photograph photograph
     :reach reach
     :sw-quantity sw-quantity}))
