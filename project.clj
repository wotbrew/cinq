(defproject com.wotbrew/cinq "0.1.0-SNAPSHOT"
  :description "Integrated query for Clojure"
  :url "https://github.com/wotbrew/cinq"
  :license {:name "MIT", :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [meander/epsilon "0.0.650"]]
  :profiles {:dev {:dependencies [[io.airlift.tpch/tpch "0.10"]
                                  [criterium "0.4.6"]
                                  [org.lmdbjava/lmdbjava "0.9.0"]]
                   :source-paths ["dev"]}}
  :jvm-opts ^:replace ["--add-opens=java.base/java.nio=ALL-UNNAMED"
                       "--add-exports" "java.base/sun.nio.ch=ALL-UNNAMED"]
  :repl-options {:init-ns cinq.core})
