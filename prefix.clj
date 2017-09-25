(ns nz-graft.prefix
  (:require [clojure.tools.logging :as log]
            [grafter.vocabularies.core :refer [prefixer]])
  (:import [java.net URI]))

;; Defines what will be useful for our next data transformations
(defn data-graph-uri->graph-uri [^URI dg-uri]
  (let [path (.getPath dg-uri)
        host (.getHost dg-uri)
        graph-slug (-> path
                       (clojure.string/replace #"data" "graph"))
        graph-uri (str "http://" host graph-slug)]
    (log/info (str "> Converted "
                   dg-uri
                   " --> "
                   graph-uri))
    (java.net.URI. graph-uri)))

(defn sanitize-slug [slug]
  "Strip out rubbish, downcase and dasherize"
  (-> slug
      clojure.string/lower-case
      clojure.string/trim
      (clojure.string/replace #" " "-")))

(def base-domain (prefixer "http://envdatapoc.co.nz"))

(def base-graph (prefixer (base-domain "/graph/")))

;; for physical things, measurement sites, council areas etc
(def base-id (prefixer (base-domain "/id/")))

(def base-vocab (prefixer (base-domain "/def/")))

(def base-data (prefixer (base-domain "/data/")))

;; other vocabs and prefixes needed
(def sosa (prefixer "http://www.w3.org/ns/sosa/"))

(def schema (prefixer "http://schema.org/"))

(def geo (prefixer "http://www.w3.org/2003/01/geo/wgs84_pos#"))

(def geosparql (prefixer "http://www.opengis.net/ont/geosparql#"))

(def xsd (prefixer "http://www.w3.org/2001/XMLSchema#"))

(def qudt-1-1 (prefixer "http://qudt.org/1.1/schema/qudt#"))

(def qudt-unit-1-1 (prefixer "http://qudt.org/1.1/vocab/unit#"))

(def nzlabs (prefixer "https://registry.scinfo.org.nz/lab/nems/def/property/"))

;; our resource/id uri generators
(defn measurement-site-uri [measurement-slug]
  "Takes the emar:LawaSiteID as a slug and returns a uri"
  (base-id (str "measurement-site/" measurement-slug)))

(defn measurement-site-geometry [measurement-slug]
  (base-id (str "measurement-site/" measurement-slug "/geometry")))

(defn wmz-geometry-uri [slug]
  (base-id (str "management-zone/" (sanitize-slug slug) "/geometry")))

(defn regional-council-uri [region-slug]
  "Taken from the emar:Region field and sanitized"
  (base-id (str "region/" (sanitize-slug region-slug))))

(defn region-geometry-uri [slug]
  (base-id (str "region/" (sanitize-slug slug) "/geometry")))

(defn mesh-uri [slug]
  (base-id (str "meshblock/" (sanitize-slug slug))))

(defn mesh-geometry-uri [slug]
  (base-id (str "meshblock/" (sanitize-slug slug) "/geometry")))

(defn observation-uri [site type time]
  "Generates uris of the form /data/HRC-00003/stage/2017-03-01T01:10:00+12:00
   based on input of the observation lawa site id, flow or stage as type and a dt string
   times are stripped of any ms value as that causes URIs invalid to rails/nginx."
  (let [sanitized-time (clojure.string/replace time #"\.0+$" "")]
    (base-data (str site "/" type "/" sanitized-time))))

(defn result-uri [site type time]
  (observation-uri site type (str time "/result")))

(defn catchment-uri [slug]
  (base-id (str "catchment/" (sanitize-slug slug))))

(defn water-management-zone-uri [slug]
  (base-id (str "management-zone/" (sanitize-slug slug))))

(defn reach-uri [slug]
  (base-id (str "reach/" (sanitize-slug slug))))

(defn reach-geometry-uri [slug]
  (base-id (str "reach/" (sanitize-slug slug) "/geometry")))

(defn node-uri [slug]
  "For either a from or to node"
  (base-id (str "node/" (sanitize-slug slug))))

;; reaches classifications

(def classification (prefixer (base-vocab "rec/")))

(defn climate-uri [slug]
  (classification (str "climate/" (sanitize-slug slug))))

(defn src-of-flow-uri [slug]
  (classification (str "source-of-flow/" (sanitize-slug slug))))

(defn geology-uri [slug]
  (classification (str "geology/" (sanitize-slug slug))))

(defn landcover-uri [slug]
  (classification (str "landcover/" (sanitize-slug slug))))

(defn net-posn-uri [slug]
  (classification (str "net-position/" (sanitize-slug slug))))

(defn vly-landfrm-uri [slug]
  (classification (str "valley-landform/" (sanitize-slug slug))))

;; long-term flow for sites

(defn long-term-flow-observation-uri [slug site-id]
  "Takes a slug and a LawaSiteID to create an observation uri.
   The LawaSiteID is not sanitized so it stays upper-case."
  (base-data (str (sanitize-slug slug) "/" site-id)))

;; types

(def sosa:FeatureOfInterest (sosa "FeatureOfInterest"))

(def nzdef:MeasurementSite (base-vocab "MeasurementSite"))

(def nzdef:WaterManagementZone (base-vocab "WaterManagementZone"))

(def nzdef:Region (base-vocab "Region"))

(def nzdef:MeshBlock (base-vocab "MeshBlock"))

(def nzdef:Reach (base-vocab "Reach"))

(def nzdef:Node (base-vocab "Node"))

(def sosa:Observation (sosa "Observation"))

;; vocabs

(def nzdef:siteID (base-vocab "siteID"))

(def geo:long (geo "long"))

(def geo:lat (geo "lat"))

(def nzdef:elevation (base-vocab "elevation"))

(def nzdef:councilSiteID (base-vocab "councilSiteID")) 

(def nzdef:lawaSiteID (base-vocab "lawaSiteID"))

(def nzdef:managementZone (base-vocab "managementZone"))

(def nzdef:catchment (base-vocab "catchment"))

(def nzdef:reach (base-vocab "reach"))

(def nzdef:reachID (base-vocab "reachID"))

(def nzdef:nodeID (base-vocab "nodeID"))

(def nzdef:fromNode (base-vocab "fromNode"))

(def nzdef:toNode (base-vocab "toNode"))

(def nzdef:order (base-vocab "order"))

(def nzdef:length (base-vocab "length"))

(def nzdef:distsea (base-vocab "distsea"))

(def nzdef:catchmentArea (base-vocab "catchmentArea"))

(def nzdef:climate (base-vocab "climate"))

(def nzdef:sourceOfFlow (base-vocab "sourceOfFlow"))

(def nzdef:geology (base-vocab "geology"))

(def nzdef:landcover (base-vocab "landcover"))

(def nzdef:netPosition (base-vocab "netPosition"))

(def nzdef:valleyLandform (base-vocab "valleyLandform"))

(def nzdef:meanAnnualFlow (base-vocab "meanAnnualFlow"))

(def nzdef:medianAnnualFlow (base-vocab "medianAnnualFlow"))

(def nzdef:minRecordedFlow (base-vocab "minRecordedFlow"))

(def nzdef:maxRecordedFlow (base-vocab "maxRecordedFlow"))

(def nzdef:meanAnnualFloodFlow (base-vocab "meanAnnualFloodFlow"))

(def nzdef:MALF (base-vocab "MALF"))

(def nzdef:MALF1d (base-vocab "MALF1d"))

(def nzdef:MALF7d (base-vocab "MALF7d"))

(def schema:image (schema "image"))

(def geosparql:hasGeometry (geosparql "hasGeometry"))

(def geosparql:Geometry (geosparql "Geometry"))

(def geosparql:asWKT (geosparql "asWKT"))

(def geosparql:wktLiteral (geosparql "wktLiteral"))

(def stage-water-level (nzlabs "stage-water-level"))

(def flow-water-level (nzlabs "flow-water-level"))

(def sosa:hasFeatureOfInterest (sosa "hasFeatureOfInterest"))

(def sosa:observedProperty (sosa "observedProperty"))

(def sosa:hasResult (sosa "hasResult"))

(def sosa:resultTime (sosa "resultTime"))

(def qudt-1-1:QuantityValue (qudt-1-1 "QuantityValue"))

(def qudt-1-1:unit (qudt-1-1 "unit"))

(def qudt-1-1:numericValue (qudt-1-1 "numericValue"))

(def qudt-unit-1-1:Millimeter (qudt-unit-1-1 "Millimeter"))

(def qudt-unit-1-1:CubicMeterPerSecond (qudt-1-1 "CubicMeterPerSecond"))

(def nzdef:LitrePerSecond (base-vocab (str "unit/LitrePerSecond")))

(def xsd:double (xsd "double"))

(def xsd:string (xsd "string"))

(def xsd:dateTime (xsd "dateTime"))

(def xsd:integer (xsd "integer"))
