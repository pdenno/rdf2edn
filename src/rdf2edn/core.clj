(ns rdf2edn.core
  "Load the datahike (DH) database from Jena content; define pathom resolvers."
  (:require
   [clojure.datafy          :refer [datafy]]
   [clojure.pprint          :refer [pprint]]
   [clojure.set             :as set]
   [clojure.string          :as str]
   [ont-app.vocabulary.core :as voc]
   [rdf2edn.aristotle       :as aris]
   [taoensso.telemere       :refer [log!]]))

;;; * I use 'dvecs' to mean the Jena data stored as a vector of 3-element vectors.
;;; * I use 'dmaps' to mean the dvecs data reorganized as a map indexed by :resource/iri or a keyword in the "temp" from Jena.
;;; * I use 'dmapv' to mean the dmaps data reorganized as a vector of maps with temps resolved.
;;; And those three bullets points pretty much describe the program architecture!

;;; ToDo:
;;;   - Don't actually write to a DB, just make the stuff people could use (objects and learned schema).
;;;     + I mean here, don't even include datahike!  This is a good idea! Call it rdf2clj.
;;;     + Drop the pathom dependency too!
;;;   - Write about API in the README
;;;   - Making resource on-the-fly: :owl/Class and :owl/Restriction (investigate). See lookup-resource

;;;  This is an atom to hold the RDF graph, useful in debugging.
(defonce graph-memo (atom nil))
(def fake-db (atom nil))
(def ^:diag diag (atom nil))

;;; (-> "project-ontologies.edn" io/resource slurp edn/read-string (core/load-graph! ggg))
(defn load-graph!
  "Given a map of entries like this:
     {:pla    {:uri \"http://www.ontologydesignpatterns.org/ont/dlp/Plans.owl\"},...
      :dol    {:uri \"http://www.ontologydesignpatterns.org/ont/dlp/DOLCE-Lite.owl\"},
      :model  {:uri \"http://modelmeth.nist.gov/modeling\", :access \"data/modeling.ttl\", :format :turtle},
      :cause  {:uri \"http://www.ontologydesignpatterns.org/ont/dlp/Causality.owl\"  :ref-only? true},
     ...}

    where
      - the keys name a RDF ns prefix,
      - :uri is a mandatory URI,
      - :access describes a path to the ontology file (.ttl etc.), and
      - :ref-only? indicates that only the alias/URI relationship is being defined (no data file),

    create an RDF graph that contains the cited data and can use the prefixes (see reg/*registry*)
    Returns an Jena respresentation of an RDF graph. Resets the core/graph-memo atom."
  [info-map]
  (let [graph (aris/graph :simple)]
    (doseq [[alias {:keys [uri access ref-only?] :as info}] info-map]
      (try
        (aris/prefix (-> alias name symbol) uri) ; See the result of this in reg/*registry*
        (if access ; the value of access is a local pathname.
          (aris/read graph access)
          (when-not ref-only? (aris/read graph (java.net.URI. uri))))
        (catch Exception e
          (let [d-e (datafy e)]
            (log! :warn (str "load-graph: alias = " alias " info = " info
                             "\nmessage: " (-> d-e :via first :message)
                             "\ncause: " (-> d-e :data :body)
                             "\ntrace: " (with-out-str (pprint (:trace d-e)))))))))
    (reset! graph-memo graph)))

;;; There is a bijection between :resource/iri and a subset of :db/id.
;;; The types of OWL things are defined in https://www.w3.org/TR/2012/REC-owl2-quick-reference-20121211/
(def static-schema
  [#:db{:ident :resource/iri       :cardinality :db.cardinality/one :valueType :db.type/keyword :unique :db.unique/identity}
   #:db{:ident :resource/name      :cardinality :db.cardinality/one :valueType :db.type/string}
   #:db{:ident :resource/namespace :cardinality :db.cardinality/one :valueType :db.type/string}
   #:db{:ident :source/short-name  :cardinality :db.cardinality/one :valueType :db.type/string  :unique :db.unique/identity}
   #:db{:ident :source/long-name   :cardinality :db.cardinality/one :valueType :db.type/string  :unique :db.unique/identity}
   #:db{:ident :source/loaded?     :cardinality :db.cardinality/one :valueType :db.type/boolean}
   #:db{:ident :box/read-str       :cardinality :db.cardinality/one :valueType :db.type/string}
   #:db{:ident :app/origin         :cardinality :db.cardinality/one :valueType :db.type/keyword}
   #:db{:ident :onto/root          :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :onto/date          :cardinality :db.cardinality/one  :valueType :db.type/string}

   ;; owl-schema --  multi-valued properties
   #:db{:ident :owl/allValuesFrom      :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/deprecated         :cardinality :db.cardinality/many :valueType :db.type/string
        :doc "ToDo: Is this actually OWL, or did I make this up?"}
   #:db{:ident :owl/disjointUnionOf    :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/disjointWith       :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/equivalentClass    :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/equivalentProperty :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/hasKey             :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/intersectionOf     :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/members            :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/onProperties       :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/oneOf              :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/propertyChainAxiom :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/sameAs             :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/someValuesFrom     :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/unionOf            :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :owl/withRestrictions   :cardinality :db.cardinality/many :valueType :db.type/ref}

   ;; owl-schema -- single-valued properties
   #:db{:ident :owl/backwardCompatibleWith :cardinality :db.cardinality/one :valueType :db.type/string}
   #:db{:ident :owl/cardinality            :cardinality :db.cardinality/one :valueType :db.type/number}
   #:db{:ident :owl/complementOf           :cardinality :db.cardinality/one :valueType :db.type/ref}
   #:db{:ident :owl/equivalentClass        :cardinality :db.cardinality/one :valueType :db.type/ref}
   #:db{:ident :owl/hasValue               :cardinality :db.cardinality/one :valueType :db.type/boolean}
   #:db{:ident :owl/imports                :cardinality :db.cardinality/one :valueType :db.type/ref}
   #:db{:ident :owl/inverseOf              :cardinality :db.cardinality/one :valueType :db.type/ref}
   #:db{:ident :owl/minCardinality         :cardinality :db.cardinality/one :valueType :db.type/number}
   #:db{:ident :owl/onProperty             :cardinality :db.cardinality/one :valueType :db.type/ref}
   #:db{:ident :owl/versionInfo            :cardinality :db.cardinality/one :valueType :db.type/string}

   ;; rdfs-schema
   ;; Where a property P has more than one rdfs:domain property, then the resources denoted by subjects of triples
   ;; with predicate P are instances of all the classes stated by the rdfs:domain properties.
   #:db{:ident :rdfs/domain        :cardinality :db.cardinality/many  :valueType :db.type/ref}
   #:db{:ident :rdfs/range         :cardinality :db.cardinality/many  :valueType :db.type/ref}
   #:db{:ident :rdfs/comment       :cardinality :db.cardinality/many :valueType :db.type/string}
   #:db{:ident :rdfs/subClassOf    :cardinality :db.cardinality/many :valueType :db.type/ref}
   #:db{:ident :rdfs/label         :cardinality :db.cardinality/one  :valueType :db.type/string}
   #:db{:ident :rdfs/subPropertyOf :cardinality :db.cardinality/one  :valueType :db.type/ref}

   ;; rdf-schema
   #:db{:ident :rdf/type      :cardinality :db.cardinality/one :valueType :db.type/ref} ; boxed because not always a keyword.
   #:db{:ident :rdf/parseType :cardinality :db.cardinality/one :valueType :db.type/keyword}]) ; e.g. :collection

(def boxed-property? "These can take multiple values of various types, so need to be boxed."
  #{:owl/disjointWith :owl/equivalentClass :owl/imports :owl/intersectionOf :owl/inverseOf :owl/onProperty :owl/oneOf
    :owl/unionOf :rdf/type :rdfs/domain :rdfs/range :rdfs/subClassOf :rdfs/subPropertyOf})

(def not-stored-property?
  "These aren't stored; they are used to create vectors."
  #{:rdf/first :rdf/rest})

(def real-keyword? ; ToDo shouldn't this reflect learning?
  "Other keywords are assumed to be resources"
  (->>
   static-schema
   (filter #(and (= (:db/valueType %) :db.type/keyword) (not= (:db/ident %) :resource/iri)))
   (map :db/ident)
   set))

(def full-schema "the above static-schema plus properties learned or specified by user in create-db!." (atom static-schema))
(def learned-schema "This is only around for diagnostics." (atom []))

(def valid-key? "This is updated by learned properties. (see learn-schema!)."
  (atom (->> static-schema (map :db/ident) set)))

(defn valid-for-transact?
  "Return false if the argument is not valid for a transaction in the system's DB.
   Should be run after learn-schema! as executed."
  [data]
  (letfn [(v4t? [obj]
            (cond (nil? obj)      (throw (ex-info "Found a nil." {}))
                  (map? obj)      (if (empty? obj)
                                    (throw (ex-info "empty map." {}))
                                    (doseq [[k v] (seq obj)]
                                      ;; @valid-key? is set of :db/ident; might include learned.
                                      (when-not (@valid-key? k)
                                        (throw (ex-info (str "Not a DB key: " k "\nobject:\n"
                                                             (with-out-str (pprint obj))) {:key k})))
                                      (v4t? v)))
                  (vector? obj)   (if (empty? obj)
                                    (throw (ex-info "empty vector." {}))
                                    (doseq [x obj] (v4t? x)))
                  (keyword? obj)  true
                  (number? obj)   true
                  (string? obj)   true
                  (boolean? obj)  true
                  :else           (throw (ex-info "Unknown object" {:obj obj}))))]
    (v4t? data)
    data))

(defn transact?
  [data]
  (valid-for-transact? data)
  (swap! fake-db into data))

(defn onto-keyword
  "Return the keyword representing an ontology or schema (e.g. RDF)."
  [sym]
  (let [[success base nam] (re-matches #"^(http://[^#]*)#?(.*)$" (str sym))]
    (when (and success (empty? nam))
      (keyword "$source" (str/replace base "/" "%")))))

(defn keywordize-triples
  "Takes a vector of 'triples-maps' with symbol keys ?x ?y ?x and returns a vector of [x y z] in which, where any of the values x, y, or z
   are resources, they are converted to either a temp using the original _<uuid>, or a map containing a keyword that uses our
   prefixes, as follows:

   From Jena (using Aristotle), all the resources are represented as either symbols starting _ or as strings matching #\"^<http://.+>$\".
      - The _ symbols are converted to keywords in the namespace 'temp'.
      - The <http://...> strings are converted to maps {:resource/temp-ref ?n}, where ?n is a keyword that uses our prefixes."
  [triples long2short]
  (letfn [(convert [v & {:keys [ref?]}]
            (cond (and (symbol? v)
                       (re-matches #"^_[0-9a-f\-]+" (name v)))        (keyword "temp" (str "t" (-> v name (subs 1))))

                  (and (string? v)
                       (re-matches #"<http://.*" v))                  (let [[success base nam]
                                                                            (re-matches #"^<(http://[^#]*)#?(.*)>$" (str v))]
                                                                        (if (and success (not-empty nam))
                                                                          (if-let [prefix (get long2short base)]
                                                                            (let [res (keyword prefix nam)]
                                                                              (if (and ref? (not= res :rdf/nil))
                                                                                {:resource/temp-ref res}
                                                                                res))
                                                                            (keyword base nam)),
                                                                          (if-let [onto-kw (onto-keyword v)]
                                                                            (if ref?
                                                                              {:resource/temp-ref (onto-keyword v)}
                                                                              onto-kw)
                                                                            (do (log! :warn (str "Keywordizing triple: ambiguous:" v))
                                                                                (keyword "BUG" (str v))))))
                  (string? v)                                         v
                  (number? v)                                         v
                  (boolean? v)                                        v
                  (keyword? v)                                        (cond (= \# (-> v name (get 0)))
                                                                            (keyword (namespace v) (-> v name (subs 1))),
                                                                            ;; :owl/import is like this. Non-readable!
                                                                            (= "" (name v)) (keyword "namespace" (namespace v)),
                                                                            :else v)
                  (= (type v) ont_app.vocabulary.lstr.LangStr)        (str v)))]
    (for [{:syms [?x ?y ?z] :as _triple} triples]
      (let [cx (convert ?x)
            cy (convert ?y)
            cz (if (real-keyword? cy) (convert ?z) (convert ?z :ref? true))]
        ;; ToDo: Write spec for triple.
        (vector cx cy cz)))))

;;;---- Operating on the keywordized dvecs -----------------------------------------------
(defn temp-id? [k]
  (and (keyword? k)
       (= "temp" (namespace k))))

(defn list-starters
  "Return a list of triples that start lists. They have a :rdf/first,
   but they aren't used as the :rdf/rest of anything."
  [dvecs]
  (->> dvecs
       (filter #(= :rdf/first (nth % 1)))
       (remove (fn [starter?]
                 (let [resource (first starter?)]
                   (some #(and (= resource (nth % 2))
                               (= :rdf/rest (nth % 1)))
                         dvecs))))))

(defn resolve-rdf-lists
  "Return a map of toplevel rdf/Lists."
  [dvecs]
  (let [list-stuff (filter #(#{:rdf/first :rdf/rest} (second %)) dvecs)
        starters (list-starters list-stuff)]
    (reduce (fn [m starter-trip]
              (assoc m
                     (first starter-trip)
                     (loop [lis []
                            resource (first starter-trip)
                            cnt 0]
                       (let [val (some #(when (and (= (nth % 0) resource)
                                                   (= (nth % 1) :rdf/first))
                                          (nth % 2))
                                       list-stuff)
                             rest-trip (some #(when (and (= (nth % 0) resource)
                                                         (= (nth % 1) :rdf/rest))
                                                %)
                                             list-stuff)]
                         (cond (= :rdf/nil (nth rest-trip 2)) (conj lis val),
                               (> cnt 100) (throw (ex-info "Didn't find RDF list termination:" {:starter-triple starter-trip})),
                               :else (recur (conj lis val) (nth rest-trip 2) (inc cnt)))))))
            {}
            starters)))

;;; ToDo: See zotero-tools/util.cljs for for .cljc style.
(defn learn-type
  "Return the :db/valueType for the data."
  [prop examples]
  (let [prop-examples (filter #(= prop (second %)) examples)
        data          (map #(nth % 2) prop-examples)
        types         (->> data (map type) distinct)
        typ           (first types)]
    (if (== 1 (count types))
      (cond (= typ clojure.lang.Keyword) :db.type/keyword,
            (= typ java.lang.String)     :db.type/string,
            (= typ java.lang.Long)       :db.type/number,
            (= typ java.lang.Boolean)    :db.type/boolean,
            (and (= typ clojure.lang.PersistentArrayMap)
                 (every? #(contains? % :resource/temp-ref) data)) :db.type/ref,
            :else (throw (ex-info "Cannot learn type for" {:data data})))
      (log! :error (str "Found multiple types while learning " prop " " types)))))

(defn learn-cardinality
  "Return either :db.cardinality/many or :db.cardinality/one based on evidence."
  [prop examples]
  (let [prop-examples (filter #(= prop (second %)) examples)
        individuals   (->> prop-examples (map first))]
     (if (== (-> individuals distinct count) (-> individuals count))
       :db.cardinality/one
       :db.cardinality/many)))

(defn learn-schema!
  "Using the dvecs, return schema maps from what we know about the owl, rdfs, and rdf parts
   plus any additional triples created by ontologies.
   It sets the atom @learned-schema and returns the value."
  [dvecs]
  (let [known-property? (->> static-schema (map :db/ident) set)
        examples (remove #(known-property? (nth % 1)) dvecs)
        unknown-properties (->> examples (mapv second) distinct (remove not-stored-property?))
        result (doall (for [prop unknown-properties]
                        #:db{:ident prop
                             :cardinality (learn-cardinality prop examples)
                             :valueType   (learn-type prop examples)
                             :app/origin :learned}))]
    (reset! learned-schema result)))

;;;--------------------------------- create and operate on the dmaps -----------------------
(defn triples2maps!
  "Iterate through the triples returning a map keyed by the RDF resource (keyword) represented.
   Expects the atom full-schema to know about learned schema."
  [dvecs rdf-lists]
  (let [single-valued-property? (->> @full-schema (filter #(= :db.cardinality/one (:db/cardinality %))) (map :db/ident) set)
        multi-valued-property?  (->> @full-schema (filter #(= :db.cardinality/many (:db/cardinality %))) (map :db/ident) set)]
    (reduce (fn [m [o a v :as triple]]
              (let [v (or (get rdf-lists v) v)]
                (when (some nil? triple) (throw (ex-info "Null value:" triple)))
                (cond (not-stored-property?    a) m,
                      (single-valued-property? a) (assoc-in m [o a] v),
                      (multi-valued-property?  a) (update-in m [o a] #(if (vector? %2) (into %1 %2) (vec (conj %1 %2))) v),
                      :else (throw (ex-info "Unknown attribute:" {:attr a})))))
            {}
            dvecs)))

(defn partition-temp-perm
  "Create a map with two keys:
     :temp-data - a map of temp resources indexed by their :resource/iri.
     :perm-data - a subset of the argument vector with temp maps removed. "
  [dmapv]
  (reduce (fn [res m]
            (let [id (:resource/iri m)]
              (if (temp-id? id)
                (assoc-in  res [:temp-data id] (dissoc m :resource/iri))
                (update res :perm-data conj m))))
          {:temp-data {} :perm-data []}
          dmapv))

(defn resolve-temp-internal
  "Temp resources can reference other temp resources.
   This resolves everything."
  [temp-maps]
  (let [progress? (atom true)]
    (letfn [(rt-aux [obj tm]
              (cond (temp-id? obj)    (if-let [v (get tm obj)]
                                        (do (reset! progress? true) v)
                                        (log! :error (str "Could not find " obj))),
                    (map?  obj)       (reduce-kv (fn [m k v] (assoc m k (rt-aux v tm))) {} obj),
                    (coll? obj)       (mapv #(rt-aux % tm) obj),
                    :else             obj))]
      (loop [tmaps temp-maps
             count 0]
        (reset! progress? false)
        (let [new-maps
              (reduce-kv (fn [m k v]
                           (assoc m k (reduce-kv (fn [mm kk vv] (assoc mm kk (rt-aux vv tmaps)))
                                                 {}
                                                 v))) ; v is a map about a temp.
                         {}
                         tmaps)]
          (cond (> count 15) (throw (ex-info "Temp data has loops." {:temp-maps temp-maps})),
                @progress?  (recur new-maps (inc count))
                :else new-maps))))))

(defn distinct-temps ; ToDo: examine how this becomes necessary.
  "The maps from resolve-temps :perm-data can contain field values that contain temps.
   The values the temps refer to ought to be unique. If they aren't this function
   returns the map with such vectors dropping the duplicates."
  [mm temps]
  (letfn [(dt-aux [vval]
            (let [seen? (atom #{})]
              (reduce (fn [res v]
                        (if (temp-id? v)
                          (if (@seen? (get temps v))
                            res
                            (do (swap! seen? conj (get temps v))
                                (conj res v)))
                          (conj res v)))
                      []
                      vval)))]
    (reduce-kv (fn [m k v]
                 (if (vector? v)
                   (assoc m k (dt-aux v))
                   (assoc m k v)))
               {}
               mm)))

(defn resolve-temps
  "Replace every temp reference with its value. Argument is a vector of maps."
  [dmapv]
  (let [{:keys [temp-data perm-data]} (partition-temp-perm dmapv)
        temp-data (resolve-temp-internal temp-data)
        perm-data (mapv #(distinct-temps % temp-data) perm-data)]
    (letfn [(rt-aux [obj]
              (cond (temp-id? obj)
                    (or (get temp-data obj) (throw (ex-info  "Could not find obj" {:obj obj}))),
                    (map? obj) (reduce-kv (fn [m k v] (assoc m k (rt-aux v))) {} obj),
                    (coll? obj) (mapv rt-aux obj),
                    :else obj))]
      (mapv rt-aux perm-data))))

(defn site-online?
  "Return true if the site reacts within timeout"
  [url timeout]
  (let [p (promise)]
    (future (deliver p (slurp url)))
    (if (string? (deref p timeout false)) true false)))

(defn lookup-resource
  "Return the :db/id of the resource given the its ns-qualified keyword identifier."
  [id]
  (or (some #(when (= id (:resource/iri %)) %) @fake-db)
      (try (log! :warn (str "Making resource on-the-fly: " id))
           (let [res {:resource/iri id
                      :resource/name (name id)
                      :resource/namespace (namespace id)}]
             (swap! fake-db conj res)
             res)
           (catch Exception e
             (log! :error (str "Error in lookup-resource: " (-> e datafy :cause)))))))

(defn resolve-temp-refs
  "resolve-temps created references to resources, :resource/temp-ref.
   Stubs for all :resource/iris were transacted to the DB.
   This recursively replaces :resource/temp-ref with their DH entity ID.
   Argument is a vector of maps, one for each resource."
  [dmapv]
  (let [save-obj (atom nil)]
    (letfn [(rtr-aux [x]
              (cond (map? x) (if (contains? x :resource/temp-ref) ; then the map has only this key; replace it.
                               (lookup-resource (:resource/temp-ref x))  ; rdf2edn
                               (reduce-kv (fn [m k v]
                                            (if (nil? v)
                                              (throw (ex-info "Failed to resolve temps:" {:obj x :k k :m m}))
                                              (assoc m k (rtr-aux v))))
                                          {} x)),
                    (vector? x) (mapv rtr-aux x),
                    (nil? x) (throw (ex-info "Failed to resolve temps:" {:save-obj @save-obj}))
                    :else x))]
      ;; We do it this way to catch errors at finer granularity.
      (mapv #(do (reset! save-obj %) (rtr-aux %)) dmapv))))

(defn store-from-info!
  "Store schema and information from the onto-info.
   Return a map of 'long2short' - URI to prefixes."
  [onto-info]
  (let [all-prefixes (merge  {:owl2 "http://www.w3.org/2006/12/owl2"
                              :rdf  "http://www.w3.org/1999/02/22-rdf-syntax-ns"
                              :rdfs "http://www.w3.org/2000/01/rdf-schema"}
                             (update-vals onto-info :uri))
        db-obj (reduce-kv (fn [res sname lname]
                            (conj res {:resource/iri (onto-keyword lname)
                                       :source/short-name (name sname)
                                       :source/long-name lname}))
                          []
                          all-prefixes)]
    (transact? db-obj)
    (-> all-prefixes
        set/map-invert
        (update-vals name))))


;;;------------------------ API functions ---------------------------------
;;;  dvecs - Jena data stored as a vector of 3-element vectors.
;;;  dmaps - dvecs data reorganized as a map indexed by :resource/iri or a keyword in the "temp" from Jena.
;;;  dmapv - dmaps data reorganized as a vector of maps with temps resolved.
;;; (core/create-db! (-> "project-ontologies.edn" io/resource slurp edn/read-string) big-cfg :reload-graph? false)
;;;  There are a few steps to this high-level API function:
;;;      1) Create some DB content based solely on the onto-info map (not the ontologies to which it refers)
;;;      2) Create the RDF graph (load-graph!); make triples (dvecs).
;;;      3) Create triples from the graph (dmaps and dmapv).
;;;      4) Learn additional DB attributes from the graph.
;;;      5) Store the triples.

;;; ToDo: I'm not handling :user-properties yet.
(defn to-edn
  "Read RDF specified by onto-infos and return edn.

   onto-info argument: a map where the keys are prefixes and the values are maps containing:
     :uri (mandatory) - a string representing the endpoint to read.
     :ref-only (optional) - the prefix will be related to the uri, but nothing will be read.
     :access (optional) - a file to read in lieu of the uri.
     :format (optional) - the type of data when using access (e.g. :turtle).

   Options map
     - :user-properties - a collection of  maps of datomic-like schema attributes to be included
       in the processing. These can be used to override learned ones.
     - :check-sites - a collection of strings to URI; If specified, we check that they are alive.
     - :check-sites-timeout - milliseconds until we give up on check sites.
     - :reload-graph? - if false it uses the graph from an earlier call."
  [onto-info & {:keys [check-sites check-sites-timeout reload-graph? _user-properties]
                       :or {check-sites-timeout 15000}}]
  (let [sites-ok? (if check-sites (every? #(site-online? % check-sites-timeout) check-sites) true)]
    (if sites-ok?
      (let [long2short (store-from-info! onto-info)
            graph (if reload-graph? (load-graph! onto-info) @graph-memo)
            jena-triple-maps (aris/run graph '[:bgp [?x ?y ?z]])
            dvecs (keywordize-triples jena-triple-maps long2short)
            learned (learn-schema! dvecs)]

        (swap! full-schema into learned)
        (swap! valid-key? into (map :db/ident learned))

        (let [rdf-lists (resolve-rdf-lists dvecs)
              dmaps (->> (triples2maps! dvecs rdf-lists) ; returns a map indexed by resource or Jena ID in a "temp" ns.
                         (reduce-kv (fn [res k v] (conj res (assoc v :resource/iri k))) []) ; give them all a :resource/iri.
                         resolve-temps)
              dmapv (resolve-temp-refs dmaps)]
          (try (valid-for-transact? dmapv)
               (catch Exception e
                 (log! :error (str "There was an error in creating the edn: " (-> e datafy :cause)))))))
      (log! :warn "Did not process read the the graph because one or more sites did not respond."))))
