(defproject com.wotbrew/cinq "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [meander/epsilon "0.0.650"]
                 [com.stuartsierra/dependency "1.0.0"]
                 [org.clojure/tools.analyzer.jvm "1.2.3"]]
  :profiles {:dev {:dependencies [[io.airlift.tpch/tpch "0.10"]
                                  [criterium "0.4.6"]]}}
  :jvm-opts ^:replace []
  :repl-options {:init-ns cinq.core})
