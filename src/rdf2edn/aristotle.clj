(ns rdf2edn.aristotle
  "Some code borrowed from https://github.com/arachne-framework/aristotle"
  (:require
   [clojure.edn :as edn]))


(defn graph [& _])

(defn prefix [& _])

(defn read [& _])

(defn run [& _]
  (->> "data/jena-triple-maps.edn" slurp (edn/read-string {:readers *data-readers*})))
