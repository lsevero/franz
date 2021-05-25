(defproject franz "1.4.3"
  :description "A http to kafka gateway"
  :url "https://github.com/lsevero/franz"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [cheshire "5.8.1"]
                 [mount "0.1.16"]
                 [aero "1.1.6"]
                 [ring/ring-jetty-adapter "1.7.1"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.3"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.3"]
                 [metosin/reitit "0.5.13"
                  :exclusions [[com.fasterxml.jackson.core/jackson-core]
                               [com.fasterxml.jackson.core/jackson-databind]
                               ]]
                 [org.apache.kafka/kafka-clients "2.6.1"
                  :exclusions [[com.fasterxml.jackson.core/jackson-core]
                               [com.fasterxml.jackson.core/jackson-databind]
                               ]]
                 [borkdude/sci "0.2.5"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.apache.logging.log4j/log4j-api "2.13.3"]
                 [org.apache.logging.log4j/log4j-core "2.13.3"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.13.3"]
                 [com.damballa/abracad "0.4.13"]
                 ]
  :main franz.core
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[cider/cider-nrepl "0.25.4"]]
                   :resource-paths ["resources"
                                    "example-config"
                                    ]
                   :jvm-opts ["-Dconfig=example-config/config.edn"]
                   :repl-options {:init-ns franz.core} 
                   }
             :uberjar {:aot :all
                       }
             } 
  )
