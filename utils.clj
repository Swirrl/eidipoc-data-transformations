(ns nz-graft.utils
  (:require [nz-graft.prefix :as pre]
            [nz-graft.transform :refer [->nz-dt-str
                                        now-in-nz
                                        yesterday-in-nz
                                        one-hour-ago-in-nz
                                        now-utc
                                        one-hour-ago-utc
                                        two-hours-ago-utc]]
            [nz-graft.remote :as remote]
            [nz-graft.uri :as nz-uri]))

;; validate those quads
(defn validate-quads [coll]
  "Strip nils from sequence and quads where there is a nil in object position."
  (->> coll
       (filter identity)
       (filter #(not (= nil
                        (:o %))))))

(defn write-pipeline-to-nt-file [absolute-path pipeline-fn]
  "Takes an absolute path to an output file WITH EXTENSION and the desired pipeline fn.
   Writes an nt file."
  (grafter.rdf/add (grafter.rdf.io/rdf-serializer absolute-path
                                                  :format
                                                  :nt)
                   (doall (pipeline-fn))))

(defn write-pipeline-to-nq-file [absolute-path pipeline-fn]
  "Takes an absolute path to an output file WITH EXTENSION and the desired pipeline fn.
   Writes an nq file."
  (grafter.rdf/add (grafter.rdf.io/rdf-serializer absolute-path
                                                  :format
                                                  :nq)
                   (doall (pipeline-fn))))
