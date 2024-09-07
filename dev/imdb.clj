(ns imdb
  "Example ns using the IMDB dataset"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.string :as str]
            [com.wotbrew.cinq.lmdb :as lmdb]
            [com.wotbrew.cinq.protocols :as p]
            [com.wotbrew.cinq :as c])
  (:import (clojure.lang IReduceInit)
           (java.util.zip GZIPInputStream)))

(set! *print-length* 100)

(defn download-tsv-files []
  (letfn [(dload [from to]
            (println "Downloading" from)
            (io/copy from to))]
    (dload "https://datasets.imdbws.com/name.basics.tsv.gz" "tmp/imdb/name.basics.tsv.gz")
    (dload "https://datasets.imdbws.com/title.akas.tsv.gz" "tmp/imdb/title.akas.tsv.gz")
    (dload "https://datasets.imdbws.com/title.basics.tsv.gz" "tmp/imdb/title.basics.tsv.gz")
    (dload "https://datasets.imdbws.com/title.crew.tsv.gz" "tmp/imdb/title.crew.tsv.gz")
    (dload "https://datasets.imdbws.com/title.episode.tsv.gz" "tmp/imdb/title.episode.tsv.gz")
    (dload "https://datasets.imdbws.com/title.principals.tsv.gz" "tmp/imdb/title.principals.tsv.gz")
    (dload "https://datasets.imdbws.com/title.ratings.tsv.gz" "tmp/imdb/title.ratings.tsv.gz")))

(defn gz-tsv [file]
  (reify
    p/Scannable
    (scan [_ f init]
      (with-open [in (io/input-stream file)
                  gz-in (GZIPInputStream. in)
                  rdr (io/reader gz-in)]
        (let [[cols & xs] (csv/read-csv rdr :separator \tab :quote \❤)
              sb (apply create-struct (map keyword cols))]
          (loop [xs xs
                 acc init
                 rsn 0]
            (if-some [[x & xs] (seq xs)]
              (let [ret (f acc nil rsn (apply struct sb x))]
                (if (reduced? ret)
                  ret
                  (recur xs ret (unchecked-inc rsn))))
              acc)))))
    IReduceInit
    (reduce [rv f init] (p/scan rv (fn [acc _ _ x] (f acc x)) init))))

(def names-tsv* (gz-tsv "tmp/imdb/name.basics.tsv.gz"))
(def akas-tsv* (gz-tsv "tmp/imdb/title.akas.tsv.gz"))
(def titles-tsv* (gz-tsv "tmp/imdb/title.basics.tsv.gz"))
(def crew-tsv* (gz-tsv "tmp/imdb/title.crew.tsv.gz"))
(def episodes-tsv* (gz-tsv "tmp/imdb/title.episode.tsv.gz"))
(def principals-tsv* (gz-tsv "tmp/imdb/title.principals.tsv.gz"))
(def ratings-tsv* (gz-tsv "tmp/imdb/title.ratings.tsv.gz"))

(defn apply-or-null [f x] (if (= "\\N" x) nil (f x)))
(defn apply-csv [f x] (apply-or-null (fn [s] (mapv #(apply-or-null f %) (str/split s #","))) x))
(defn apply-space [f x] (apply-or-null (fn [s] (mapv #(apply-or-null f %) (str/split s #" "))) x))

(defn parse-id [s] (parse-long (subs s 2)))

(def names-tsv
  (c/rel [{:keys [nconst primaryName birthYear deathYear primaryProfession knownForTitles]} names-tsv*]
         {:nconst (parse-id nconst)
     :primaryName primaryName
     :birthYear (apply-or-null parse-long birthYear)
     :deathYear (apply-or-null parse-long deathYear)
     :primaryProfession (apply-csv keyword primaryProfession)
     :knownForTitles (apply-csv parse-id knownForTitles)}))

(def akas-tsv
  (c/rel [{:keys [titleId ordering title region language types attributes isOriginalTitle]} akas-tsv*]
         {:titleId (parse-id titleId)
     :ordering (apply-or-null parse-long ordering)
     :title title
     :region (apply-or-null keyword region)
     :language (apply-or-null keyword language)
     :types (apply-csv keyword types)
     :attributes (apply-space str attributes)
     :isOriginalTitle (= "1" isOriginalTitle)}))

(def titles-tsv
  (c/rel [{:keys [tconst titleType primaryTitle originalTitle isAdult startYear endYear runtimeMinutes genres]} titles-tsv*]
         {:tconst (parse-id tconst)
     :titleType (apply-or-null keyword titleType)
     :primaryTitle primaryTitle
     :originalTitle originalTitle
     :isAdult (= "1" isAdult)
     :startYear (apply-or-null parse-long startYear)
     :endYear (apply-or-null parse-long endYear)
     :runtimeMinutes (apply-or-null parse-long runtimeMinutes)
     :genres (apply-csv keyword genres)}))

(def crew-tsv
  (c/rel [{:keys [tconst directors writers]} crew-tsv*]
         {:tconst (parse-id tconst)
     :directors (apply-csv parse-id directors)
     :writers (apply-csv parse-id writers)}))

(def episodes-tsv
  (c/rel [{:keys [tconst parentTconst seasonNumber episodeNumber]} episodes-tsv*]
         {:tconst (parse-id tconst)
     :parentTconst (parse-id parentTconst)
     :seasonNumber (apply-or-null parse-long seasonNumber)
     :episodeNumber (apply-or-null parse-long episodeNumber)}))

(def principals-tsv
  (c/rel [{:keys [tconst ordering nconst category job characters]} principals-tsv*]
         {:tconst (parse-id tconst)
     :ordering (apply-or-null parse-long ordering)
     :nconst (parse-id nconst)
     ;; are these csv?
     :category (apply-or-null keyword category)
     :job (apply-or-null str job)
     :characters (apply-or-null str characters)}))

(def ratings-tsv
  (c/rel [{:keys [tconst averageRating numVotes]} ratings-tsv*]
         {:tconst (parse-id tconst)
     :averageRating (apply-or-null parse-double averageRating)
     :numVotes (apply-or-null parse-long numVotes)}))

(def tsv-db
  {:names names-tsv
   :akas akas-tsv
   :titles titles-tsv
   :crew crew-tsv
   :episodes episodes-tsv
   :principals principals-tsv
   :ratings ratings-tsv})

(comment
  (io/delete-file "tmp/imdb.cinq" true)
  (def lmdb (lmdb/database "tmp/imdb.cinq"))
  (.close lmdb)
  )

(defonce lmdb (lmdb/database "tmp/imdb.cinq"))

(defn load-tsv-data-into-lmdb []
  (doseq [[k tsv] tsv-db
          :let [rel (get lmdb k)]]
    (when-not (c/rel-first rel)
      (println "Loading" k)
      (time (c/rel-set rel tsv))
      (println "Loaded" (c/rel-count rel) k "rows"))))

(comment

  (load-tsv-data-into-lmdb)

  )

(def names (c/create lmdb :names))
(def akas (c/create lmdb :akas))
(def titles (c/create lmdb :titles))
(def crew (c/create lmdb :crew))
(def episodes (c/create lmdb :episodes))
(def principals (c/create lmdb :principals))
(def ratings (c/create lmdb :ratings))


(comment
  (time (c/index names :nconst))
  (time (c/index principals :tconst))
  (time (c/index titles :primaryTitle))

  (c/rel-count names)

  (c/rel-count akas)

  (c/rel-count titles)

  (c/rel-count crew)

  (c/rel-count episodes)

  (c/rel-count principals)

  (c/rel-count ratings)

  (c/rel-count (:lmdb/symbols lmdb))

  (:lmdb/variables lmdb)

  )

(defn film-cast [{:keys [titles principals names]} title]
  (c/rel [t titles
        :when (and (= :movie t:titleType) (= title t:primaryTitle))
        :limit 1
        :join [p principals (= p:tconst t:tconst)]
        :join [n names (= p:nconst n:nconst)]]
         n:primaryName))

(defn film-cast-nlj [{:keys [titles principals names]} title]
  (c/rel [t (c/lookup titles :primaryTitle title)
        :when (= :movie t:titleType)
        :limit 1
        p (c/lookup principals :tconst t:tconst)
        n (c/lookup names :nconst p:nconst)]
         n:primaryName))

(comment

  (doseq [[k tsv] tsv-db
          :let [rel (get lmdb k)]]
    (when-not (c/rel-first rel) (c/rel-set rel tsv)))

  (require 'clj-async-profiler.core)
  (clj-async-profiler.core/serve-ui "127.0.0.1" 5001)

  (c/rel [t titles-tsv
        :when (and (= :movie t:titleType)
                   (= "Alien" t:primaryTitle))
        :limit 1
        p principals-tsv
        :when (= p:tconst t:tconst)
        n names-tsv
        :when (= p:nconst n:nconst)]
         n:primaryName)

  (time (vec (film-cast tsv-db "Alien")))
  (time (vec (film-cast lmdb "Alien")))

  (require 'criterium.core)
  (criterium.core/quick-bench
    (vec (film-cast-nlj lmdb "Alien"))
    )
  (criterium.core/quick-bench
    (vec (film-cast-nlj lmdb "Blade Runner"))
    )

  (set (c/rel [t titles] t:titleType))

  (c/rel [t titles
        :when (and (= :videoGame t:titleType)
                   (str/starts-with? t:primaryTitle "Star Wars"))]
         [t:primaryTitle (c/scalar [r ratings :when (= r:tconst t:tconst)] r:averageRating)])

  (c/rel [t titles
        :when (and (= t:titleType :movie) (< 1990 t:startYear 2001))
        :join [r ratings (= t:tconst r:tconst)]
        :when (and (< 8.0 r:averageRating) (< 10000 r:numVotes))
        :order [r:averageRating :desc]
        :limit 10]
         t:primaryTitle)

  )
