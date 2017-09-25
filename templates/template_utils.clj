(ns nz-graft.templates.template-utils
    (:require
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [nz-graft.prefix :as pre]
     [nz-graft.transform :refer [->integer ->nz-dt-str now-in-nz yesterday-in-nz litre-per-second->cumec]]))

(defn literal-if-present [value literal-uri]
  "Returns nil if the value is not present
   Meaning it will be caught by the validator
   otherwise an empty string with a uri is truthy"
  (if value
    (grafter.rdf/literal value
                         literal-uri)))

(defn long-term-flow-resource [resource-uri value]
  (when resource-uri
    [resource-uri
     [rdf:a pre/qudt-1-1:QuantityValue]
     [pre/qudt-1-1:numericValue (grafter.rdf/literal value
                                                     pre/xsd:double)]
     [pre/qudt-1-1:unit pre/qudt-unit-1-1:CubicMeterPerSecond]]))