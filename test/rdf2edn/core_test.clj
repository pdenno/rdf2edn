(ns rdf2edn.core-test
  "Testing auto-created Pathom3 resolvers Note comment below about the need to build the DB before testing."
  (:require
   [clojure.pprint                :refer [cl-format pprint]]
   [clojure.edn                   :as edn]
   [clojure.java.io               :as io]
   [clojure.test :refer  [deftest is testing]]
   [ont-app.vocabulary.core :as voc]
   [rdf2edn.core      :as core]))

;;; THIS is the namespace I am hanging out in recently.
(def ^:diag diag (atom nil))

(def alias? (atom (-> (ns-aliases *ns*) keys set)))

(defn ^:diag ns-start-over!
  "This one has been useful. If you get an error evaluate this ns, (the declaration above) run this and try again."
  []
  (map (partial ns-unalias *ns*) (keys (ns-aliases *ns*))))

(defn ^:diag remove-alias
  "This one has NOT been useful!"
  [al ns-sym]
  (swap! alias? (fn [val] (->> val (remove #(= % al)) set)))
  (ns-unalias (find-ns ns-sym) al))

(defn safe-alias
  [al ns-sym]
  (when (and (not (@alias? al))
             (find-ns ns-sym))
    (alias al ns-sym)))

(defn ^:diag ns-setup!
  "Use this to setup useful aliases for working in this NS."
  []
  (ns-start-over!)
  (reset! alias? (-> (ns-aliases *ns*) keys set))
  (safe-alias 's      'clojure.spec.alpha)
  (safe-alias 'io     'clojure.java.io)
  (safe-alias 'str    'clojure.string)
  (safe-alias 'voc    'ont-app.vocabulary.core)
  (safe-alias 'core   'rdf2edn.core)
  (safe-alias 'tel    'taoensso.telemere))

(defonce big-sources (-> "data/project-ontologies.edn" slurp edn/read-string))

;;; (make-big-db big-cfg) ;; After the first usage, you can remove the options :load-graph? and :check-status.
(defn ^:diag make-big-db
  "This establishes the DB. It takes a minute or two."
  []
  (core/to-edn
   big-sources
   :reload-graph? true
   :check-sites ["http://ontologydesignpatterns.org/wiki/Main_Page"]))

;;; I used this to output intermediate stuff from aristotle.
;;; (backup-triples (q/run @core/graph-memo '[:bgp [?x ?y ?z]]))
(defn ^:diag backup-triples
  "Write the triples to edn."
  [triples]
  (with-open [out (io/writer "data/jena-triple-maps.edn")]
    (cl-format out "~%[")
    (doseq [obj triples] (cl-format out "~%~A" (with-out-str (pprint obj))))
    (cl-format out "~%]")))

;;; (backup-finished-triples @core/diag)
(defn ^:diag backup-finished-maps
  "Write the dmapv triples to edn."
  [triples]
  (with-open [out (io/writer "data/finished-maps.edn")]
    (cl-format out "~%[")
    (doseq [obj triples] (cl-format out "~%~A" (with-out-str (pprint obj))))
    (cl-format out "~%]")))
