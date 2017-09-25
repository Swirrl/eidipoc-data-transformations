(ns nz-graft.uri
  (:require [ring.util.codec :as codec]))

(def horizons-monitoring-sites "http://gis.horizons.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=getfeature&service=WFS&typename=MonitoringSiteReferenceData")
 
(def canterbury-monitoring-sites "http://gis.ecan.govt.nz/arcgis/services/Public/LAWA/MapServer/WFSServer?version=1.1.0&request=GetFeature&service=WFS&typename=emar:MonitoringSiteReferenceData&srsName=EPSG:4326")

(def hawkes-bay-monitoring-sites "https://hbrcwebmap.hbrc.govt.nz/arcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=GetFeature&service=WFS&typename=MonitoringSiteReferenceData&srsName=urn:ogc:def:crs:EPSG:6.9:4326&Version=1.1.0")

(def waikato-monitoring-sites "http://wrcgis.waikatoregion.govt.nz/wrcgis/services/emar/MonitoringSiteReferenceData/MapServer/WFSServer?request=Getfeature&service=WFS&VERSION=1.1.0&typename=monitoringsitereferencedata&srsname=epsg:4326")

(defn monitoring-feed-params [org feature-of-interest property from to]
  "The NZ GIS system has predictable URIs for locations and times
   when working with monitoring station feeds. This function takes
   a hash map and uses this to create the key value pairs that will
   return the correct feed of XML. This can then be encoded into a
   query string later"

  (let [observed-property (case org
                            :horizons (if (= property :stage)
                                        "Stage [Water Level]"
                                        "Flow [Water Level]")
                            :canterbury (if (= property :stage)
                                          "Stage [Water Level]"
                                          "Flow [Flow]")
                            :hawkes-bay (if (= property :stage)
                                          "Stage [Water Level]"
                                          "Flow [Water Level]")
                            :waikato (if (= property :stage)
                                          "Water Level"
                                          "Discharge"))
        standard-mapping {:service "SOS"
                          :request "GetObservation"
                          :featureOfInterest feature-of-interest
                          :observedProperty observed-property
                          :temporalFilter (str "om:phenomenonTime,"
                                               from
                                               "/"
                                               to)}
        
        mapping (case org
                  :horizons standard-mapping
                  :canterbury {:service "SOS"
                               :request "GetObservation"
                               :featureOfInterest feature-of-interest
                               :observedProperty observed-property
                               :temporalFilter (str "om:phenomenonTime,"
                                                    from)}
                  :hawkes-bay standard-mapping
                  :waikato {:service "SOS"
                            :request "GetObservation"
                            :featureOfInterest feature-of-interest
                            :procedure "Cmd.P"
                            :observedProperty observed-property
                            :temporalFilter (str "om:phenomenonTime,"
                                                 from
                                                 "/"
                                                 to)})]
    mapping))

;; prefixes for the different council/regions' GIS systems
;; after the .govt.nz the slugs should be the same but
;; in practice only the query string is the same
(def org->monitoring-site-prefix
  {:horizons "http://tsdata.horizons.govt.nz/boo.hts"
   :canterbury "http://wateruse.ecan.govt.nz/Telemetry.hts"
   :hawkes-bay "http://data.hbrc.govt.nz/EnviroData/EMAR.hts"
   :waikato "http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS"})

;; get the uri to return XML feed
;; takes an org and a hash of the params to generate the query string with
;; :property should be :stage or :flow
(defn get-site-feed-uri [org {feature-of-interest :feature-of-interest
                              property :property
                              from :from
                              to :to}]
  
  (str (org->monitoring-site-prefix org)
       "?"
       (-> (monitoring-feed-params org feature-of-interest property from to)
           codec/form-encode
           (clojure.string/replace #"\+" "%20"))))

(def example-data-point-uri "http://tsdata.horizons.govt.nz/boo.hts?service=SOS&request=GetObservation&featureOfInterest=Manawatu%20at%20Teachers%20College&observedProperty=Stage%20[Water%20Level]&temporalFilter=om:phenomenonTime,2017-03-01T00:00:00/2017-03-14T00:00:00")

(def second-example-data-point-uri "http://data.hbrc.govt.nz/EnviroData/EMAR.hts?service=SOS&request=GetObservation&featureOfInterest=Aniwaniwa Stream at Aniwaniwa&observedProperty=Stage [Water Level]&temporalFilter=om:phenomenonTime,2017-03-01T00:00:00/2017-03-14T00:00:00")

