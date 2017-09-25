(ns nz-graft.transform
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.local :as l]))

(defn ->integer
  "An example transformation function that converts a string to an integer"
  [s]
  (Integer/parseInt s))

(defn ->dt
  "Create a datetime object from year, month, day args"
  [year month day]
  (t/date-time year month day))

(defn ->dt-str
  "Create a datetime string from y m d"
  [year month day]
  (-> (->dt year month day)
      str))

(defn ->offset-dt
  "Create a dt with offset"
  [year month day offset]
  (t/from-time-zone (t/date-time year month day)
                    (t/time-zone-for-offset offset)))

(defn ->nz-dt-str
  [year month day]
  (->offset-dt year month day 12))

(defn now-in-nz []
  (t/from-time-zone (t/now)
                    (t/time-zone-for-offset 12)))

(defn yesterday-in-nz []
  "All my troubles, eh bro...
   okay, I'll get my coat."
  (t/from-time-zone (t/yesterday)
                    (t/time-zone-for-offset 12)))

(defn one-hour-ago-in-nz []
  (-> (t/now)
      (t/minus (t/hours 1))
      (t/from-time-zone (t/time-zone-for-offset 12))))

(defn concrete-utc-plus-12 []
  (-> (t/now)
      (t/plus (t/hours 12))))

(defn now-utc []
  (t/now))

(defn two-days-ago-utc []
  (-> (now-utc)
      (t/minus (t/days 2))))

(defn one-hour-ago-utc []
  (-> (now-utc)
      (t/minus (t/hours 1))))

(defn two-hours-ago-utc []
  (-> (now-utc)
      (t/minus (t/hours 2))))

(defn sanitize-time-for [time org]
  "Rails can't handle ms in times and some orgs give us
   ms in their results. Parse their output into a local (absolute) time
   as opposed to parsing it into the equivalent UTC time (as denoted by trailing Z)
   then manually add the offset back when unparsing to a string.
   It's also worth noting that we assume whatever the offset is that comes from
   their server is correct, which might not actually be the case."
  (let [formatter (if (= org :waikato)
                    :date-time
                    :date-time-no-ms)
        offset (->> (clojure.string/split time #"\+")
                   second
                   (str "+"))
        parsed-time (f/parse-local (f/formatters formatter) time)]
    
    (-> (f/unparse-local (f/formatters :date-time-no-ms) parsed-time)
        (str offset))))

(defn litre-per-second->cumec [val]
  "Takes a value as a string and converts it into a double
   then divides by 1000 to give the m3 result"
  (-> val
      Double/parseDouble
      (/ 1000)))


