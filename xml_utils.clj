(ns nz-graft.xml-utils)

;; utils for xml
(defn filter-by-tag [kw coll] 
  (filter #(= (:tag %) kw) coll))

(defn first-with-tag [kw coll]
  (-> (filter-by-tag kw coll)
      first))

(defn first-content-by-tag [kw coll]
  "Filter returns a seq so to access via key we generally want
   to return the one match and then call :content to descend further."
  (-> (first-with-tag kw coll)
      :content))

(defmacro descend-xml-tree [vec-of-kws xml-structure]
  "Given a vector of keywords finds the first match for each and descends the tree.
   Assumes you are starting from the root. Returns a seq due to the underlying lib."
  (reverse ;; flip whole assembled list
   (into (reverse ;; counteract conj from into call
          `(->> ~xml-structure
                :content))
         (map (fn [kw] `(first-content-by-tag ~kw)) vec-of-kws))))
